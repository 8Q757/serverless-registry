import { Env } from "../..";
import { wrap } from "../utils";
import { BlobUnknownError, ManifestUnknownError } from "../v2-errors";
import { GarbageCollectionMode } from "./garbage-collector";
import {
  CheckLayerResponse,
  CheckManifestResponse,
  FinishedUploadObject,
  GetLayerResponse,
  GetManifestResponse,
  ListRepositoriesResponse,
  ListTagsResponse,
  PutManifestResponse,
  Registry,
  RegistryError,
  UploadId,
  UploadObject,
  wrapError,
} from "./registry";

// Read-only R2 registry that understands zot's OCI Image Layout on-disk format.
// zot writes:
//   <root>/<repo>/oci-layout                                -> {"imageLayoutVersion":"1.0.0"}
//   <root>/<repo>/index.json                                -> list of manifests with tag annotations
//   <root>/<repo>/blobs/<algo>/<hex>                        -> manifests AND layer blobs
//
// <root> defaults to "zot/zot" (matches zot config rootdirectory="/zot" with its
// internal extra "/zot" segment). Override via env.ZOT_ROOT_PREFIX if different.

const DEFAULT_ROOT = "zot/zot";
const OCI_LAYOUT_MARKER = "oci-layout";
const INDEX_JSON = "index.json";
const TAG_ANNOTATION = "org.opencontainers.image.ref.name";
const DEFAULT_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json";

type OciIndex = {
  schemaVersion: number;
  manifests: Array<{
    mediaType: string;
    digest: string;
    size: number;
    annotations?: Record<string, string>;
  }>;
};

function splitDigest(digest: string): { algo: string; hex: string } {
  const i = digest.indexOf(":");
  if (i < 0) return { algo: "sha256", hex: digest };
  return { algo: digest.slice(0, i), hex: digest.slice(i + 1) };
}

function manifestNotFound(): RegistryError {
  return { response: new Response(JSON.stringify(ManifestUnknownError), { status: 404 }) };
}

function blobNotFound(): RegistryError {
  return { response: new Response(JSON.stringify(BlobUnknownError), { status: 404 }) };
}

function notImplemented(op: string): RegistryError {
  return {
    response: new Response(
      JSON.stringify({
        errors: [{ code: "UNSUPPORTED", message: `${op} is not supported in zot-read-only mode`, detail: null }],
      }),
      { status: 501, headers: { "Content-Type": "application/json" } },
    ),
  };
}

export class ZotR2Registry implements Registry {
  constructor(private env: Env) {}

  private root(): string {
    return this.env.ZOT_ROOT_PREFIX || DEFAULT_ROOT;
  }

  private indexKey(repo: string): string {
    return `${this.root()}/${repo}/${INDEX_JSON}`;
  }

  private blobKey(repo: string, digest: string): string {
    const { algo, hex } = splitDigest(digest);
    return `${this.root()}/${repo}/blobs/${algo}/${hex}`;
  }

  private async readIndex(repo: string): Promise<OciIndex | null> {
    const [obj, err] = await wrap(this.env.REGISTRY.get(this.indexKey(repo)));
    if (err || !obj) return null;
    try {
      return await obj.json<OciIndex>();
    } catch {
      return null;
    }
  }

  private findByTag(index: OciIndex, tag: string) {
    return index.manifests.find((m) => m.annotations?.[TAG_ANNOTATION] === tag);
  }

  private findByDigest(index: OciIndex, digest: string) {
    return index.manifests.find((m) => m.digest === digest);
  }

  // Resolve a reference (tag or digest) to a known entry {digest, mediaType?, size?}.
  // For digest lookups not in index.json (e.g. platform-specific sub-manifests of an
  // index), we return just the digest and the caller must detect mediaType later.
  private async resolveRef(
    repo: string,
    reference: string,
  ): Promise<{ digest: string; mediaType?: string; size?: number } | null> {
    if (reference.startsWith("sha256:") || reference.includes(":")) {
      const digest = reference;
      const idx = await this.readIndex(repo);
      const entry = idx ? this.findByDigest(idx, digest) : undefined;
      if (entry) return { digest, mediaType: entry.mediaType, size: entry.size };
      return { digest };
    }
    const idx = await this.readIndex(repo);
    if (!idx) return null;
    const entry = this.findByTag(idx, reference);
    if (!entry) return null;
    return { digest: entry.digest, mediaType: entry.mediaType, size: entry.size };
  }

  // ===== Read operations =====

  async manifestExists(name: string, reference: string): Promise<CheckManifestResponse | RegistryError> {
    const resolved = await this.resolveRef(name, reference);
    if (!resolved) return { exists: false };

    const [head, err] = await wrap(this.env.REGISTRY.head(this.blobKey(name, resolved.digest)));
    if (err) return wrapError("manifestExists", err);
    if (!head) return { exists: false };

    return {
      exists: true,
      digest: resolved.digest,
      contentType: resolved.mediaType ?? DEFAULT_MANIFEST_MEDIA_TYPE,
      size: resolved.size ?? head.size,
    };
  }

  async getManifest(name: string, reference: string): Promise<GetManifestResponse | RegistryError> {
    const resolved = await this.resolveRef(name, reference);
    if (!resolved) return manifestNotFound();

    const [obj, err] = await wrap(this.env.REGISTRY.get(this.blobKey(name, resolved.digest)));
    if (err) return wrapError("getManifest", err);
    if (!obj) return manifestNotFound();

    // If we don't know mediaType (digest lookup for a sub-manifest not listed in
    // index.json), buffer the body and detect from its own mediaType field.
    if (!resolved.mediaType) {
      const text = await obj.text();
      let mediaType = DEFAULT_MANIFEST_MEDIA_TYPE;
      try {
        const parsed = JSON.parse(text);
        if (typeof parsed.mediaType === "string") mediaType = parsed.mediaType;
      } catch {
        // fall through with default
      }
      const bytes = new TextEncoder().encode(text);
      return {
        stream: new Response(bytes).body!,
        digest: resolved.digest,
        size: bytes.byteLength,
        contentType: mediaType,
      };
    }

    return {
      stream: obj.body!,
      digest: resolved.digest,
      size: resolved.size ?? obj.size,
      contentType: resolved.mediaType,
    };
  }

  async layerExists(name: string, digest: string): Promise<CheckLayerResponse | RegistryError> {
    const [head, err] = await wrap(this.env.REGISTRY.head(this.blobKey(name, digest)));
    if (err) return wrapError("layerExists", err);
    if (!head) return { exists: false };
    return {
      exists: true,
      digest,
      size: head.size,
    };
  }

  async getLayer(name: string, digest: string): Promise<GetLayerResponse | RegistryError> {
    const [obj, err] = await wrap(this.env.REGISTRY.get(this.blobKey(name, digest)));
    if (err) return wrapError("getLayer", err);
    if (!obj) return blobNotFound();
    return {
      stream: obj.body!,
      digest,
      size: obj.size,
    };
  }

  async listTags(name: string, n: number, last?: string): Promise<ListTagsResponse | RegistryError> {
    const idx = await this.readIndex(name);
    if (!idx) return { name, tags: [] };
    const all = idx.manifests
      .map((m) => m.annotations?.[TAG_ANNOTATION])
      .filter((t): t is string => typeof t === "string" && t.length > 0)
      .sort();
    const startIdx = last ? all.findIndex((t) => t > last) : 0;
    const effectiveStart = startIdx < 0 ? all.length : startIdx;
    const window = all.slice(effectiveStart, effectiveStart + n);
    return {
      name,
      tags: window,
      truncated: effectiveStart + n < all.length,
    };
  }

  async listRepositories(limit?: number, last?: string): Promise<ListRepositoriesResponse | RegistryError> {
    // Walk the bucket looking for `<root>/<repo>/oci-layout` markers.
    // Each such marker identifies one repository; extract <repo> from the key.
    const prefix = `${this.root()}/`;
    const markerSuffix = `/${OCI_LAYOUT_MARKER}`;
    const max = limit ?? 1000;
    const repositories: string[] = [];
    let cursor: string | undefined = undefined;
    let lastKey: string | undefined = undefined;
    const startAfter = last ? `${prefix}${last}${markerSuffix}` : undefined;

    while (repositories.length < max) {
      const listPromise: Promise<R2Objects> = this.env.REGISTRY.list({
        prefix,
        limit: 1000,
        cursor,
        startAfter: cursor ? undefined : startAfter,
      });
      const [res, err] = await wrap(listPromise);
      if (err) return wrapError("listRepositories", err);
      if (!res) break;

      for (const obj of res.objects) {
        lastKey = obj.key;
        if (!obj.key.endsWith(markerSuffix)) continue;
        const repo = obj.key.slice(prefix.length, obj.key.length - markerSuffix.length);
        if (!repo) continue;
        repositories.push(repo);
        if (repositories.length >= max) break;
      }

      if (!res.truncated) break;
      cursor = res.cursor;
    }

    return {
      repositories,
      cursor: repositories.length > 0 ? repositories[repositories.length - 1] : lastKey,
    };
  }

  // ===== Write operations: all stubbed =====
  // Routes still exist in router.ts; they'll return 501 if the client tries to push.
  // Expected deployment: push traffic is routed to a zot instance via CF rules,
  // and this Worker only sees pull traffic. These stubs are a safety net.

  async putManifest(
    _namespace: string,
    _reference: string,
    _stream: ReadableStream<any>,
    _opts: { contentType: string; checkLayers?: boolean },
  ): Promise<PutManifestResponse | RegistryError> {
    return notImplemented("putManifest");
  }

  async mountExistingLayer(
    _sourceName: string,
    _digest: string,
    _destinationName: string,
  ): Promise<RegistryError | FinishedUploadObject> {
    return notImplemented("mountExistingLayer");
  }

  async startUpload(_namespace: string): Promise<UploadObject | RegistryError> {
    return notImplemented("startUpload");
  }

  async getUpload(_namespace: string, _uploadId: string): Promise<UploadObject | RegistryError> {
    return notImplemented("getUpload");
  }

  async cancelUpload(_namespace: string, _uploadId: UploadId): Promise<true | RegistryError> {
    return notImplemented("cancelUpload");
  }

  async monolithicUpload(
    _namespace: string,
    _expectedSha: string,
    _stream: ReadableStream,
    _size?: number,
  ): Promise<FinishedUploadObject | RegistryError | false> {
    return notImplemented("monolithicUpload");
  }

  async uploadChunk(
    _namespace: string,
    _uploadId: string,
    _location: string,
    _stream: ReadableStream,
    _length?: number,
    _range?: [number, number],
  ): Promise<UploadObject | RegistryError> {
    return notImplemented("uploadChunk");
  }

  async finishUpload(
    _namespace: string,
    _uploadId: string,
    _location: string,
    _expectedDigest: string,
    _stream?: ReadableStream,
    _length?: number,
  ): Promise<FinishedUploadObject | RegistryError> {
    return notImplemented("finishUpload");
  }

  async garbageCollection(_namespace: string, _mode: GarbageCollectionMode): Promise<boolean> {
    return false;
  }
}

