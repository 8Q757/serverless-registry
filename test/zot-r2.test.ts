import { describe, expect, test } from "vitest";
import { env } from "cloudflare:test";
import { Env } from "..";
import { ZotR2Registry } from "../src/registry/zot-r2";

// We bypass the HTTP router entirely and drive ZotR2Registry directly against the
// miniflare-provided R2 mock. Fixtures mimic what zot writes in OCI Image Layout:
//
//   <root>/<repo>/oci-layout                    -> {"imageLayoutVersion":"1.0.0"}
//   <root>/<repo>/index.json                    -> { manifests: [ { digest, mediaType, annotations: { ref.name: <tag> } } ] }
//   <root>/<repo>/blobs/sha256/<hex>            -> manifest JSON or layer bytes

const ROOT = "zot/zot"; // matches DEFAULT_ROOT in zot-r2.ts
const TAG_ANNOTATION = "org.opencontainers.image.ref.name";

async function sha256Hex(data: Uint8Array | string): Promise<string> {
  const bytes = typeof data === "string" ? new TextEncoder().encode(data) : data;
  const buf = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(buf))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

async function putBlob(repo: string, body: string | Uint8Array): Promise<string> {
  const bytes = typeof body === "string" ? new TextEncoder().encode(body) : body;
  const hex = await sha256Hex(bytes);
  await env.REGISTRY.put(`${ROOT}/${repo}/blobs/sha256/${hex}`, bytes);
  return `sha256:${hex}`;
}

async function putIndex(
  repo: string,
  manifests: Array<{ digest: string; mediaType: string; size: number; tag?: string }>,
): Promise<void> {
  const index = {
    schemaVersion: 2,
    manifests: manifests.map((m) => ({
      mediaType: m.mediaType,
      digest: m.digest,
      size: m.size,
      ...(m.tag ? { annotations: { [TAG_ANNOTATION]: m.tag } } : {}),
    })),
  };
  await env.REGISTRY.put(`${ROOT}/${repo}/index.json`, JSON.stringify(index));
  await env.REGISTRY.put(`${ROOT}/${repo}/oci-layout`, JSON.stringify({ imageLayoutVersion: "1.0.0" }));
}

describe("ZotR2Registry", () => {
  const zot = new ZotR2Registry(env as Env);

  test("getLayer returns the blob for a given digest", async () => {
    const payload = "layer-bytes-" + Math.random();
    const digest = await putBlob("myrepo", payload);

    const res = await zot.getLayer("myrepo", digest);
    expect("stream" in res).toBe(true);
    if (!("stream" in res)) return;
    expect(res.digest).toBe(digest);
    const text = await new Response(res.stream).text();
    expect(text).toBe(payload);
  });

  test("getLayer returns 404 when blob missing", async () => {
    const res = await zot.getLayer("myrepo", "sha256:" + "00".repeat(32));
    expect("response" in res).toBe(true);
    if ("response" in res) {
      expect(res.response.status).toBe(404);
    }
  });

  test("layerExists reports true for existing blob, false for missing", async () => {
    const digest = await putBlob("myrepo", "hello");

    const hit = await zot.layerExists("myrepo", digest);
    expect(hit).toMatchObject({ exists: true, digest });

    const miss = await zot.layerExists("myrepo", "sha256:" + "ff".repeat(32));
    expect(miss).toEqual({ exists: false });
  });

  test("getManifest by tag resolves via index.json annotations", async () => {
    const manifestJson = JSON.stringify({
      schemaVersion: 2,
      mediaType: "application/vnd.oci.image.manifest.v1+json",
      config: { mediaType: "application/vnd.oci.image.config.v1+json", digest: "sha256:aa", size: 2 },
      layers: [],
    });
    const digest = await putBlob("alpine", manifestJson);
    await putIndex("alpine", [
      { digest, mediaType: "application/vnd.oci.image.manifest.v1+json", size: manifestJson.length, tag: "test" },
    ]);

    const res = await zot.getManifest("alpine", "test");
    expect("stream" in res).toBe(true);
    if (!("stream" in res)) return;
    expect(res.digest).toBe(digest);
    expect(res.contentType).toBe("application/vnd.oci.image.manifest.v1+json");
    const body = await new Response(res.stream).text();
    expect(JSON.parse(body)).toMatchObject({ schemaVersion: 2 });
  });

  test("getManifest by tag returns 404 when tag missing", async () => {
    await putIndex("empty", []);

    const res = await zot.getManifest("empty", "nonexistent");
    expect("response" in res).toBe(true);
    if ("response" in res) {
      expect(res.response.status).toBe(404);
    }
  });

  test("getManifest by digest works when listed in index.json", async () => {
    const body = JSON.stringify({ schemaVersion: 2, mediaType: "application/vnd.oci.image.manifest.v1+json" });
    const digest = await putBlob("r", body);
    await putIndex("r", [{ digest, mediaType: "application/vnd.oci.image.manifest.v1+json", size: body.length, tag: "t" }]);

    const res = await zot.getManifest("r", digest);
    expect("stream" in res).toBe(true);
    if (!("stream" in res)) return;
    expect(res.digest).toBe(digest);
    expect(res.contentType).toBe("application/vnd.oci.image.manifest.v1+json");
    await new Response(res.stream).text(); // drain stream
  });

  test("getManifest by digest for a child manifest (not in index.json) detects mediaType from body", async () => {
    // Simulate a manifest-list referencing platform manifests. The platform
    // manifest has a digest and mediaType in its own JSON, but is not a
    // top-level index.json entry.
    const childBody = JSON.stringify({
      schemaVersion: 2,
      mediaType: "application/vnd.docker.distribution.manifest.v2+json",
      config: { digest: "sha256:cc", size: 1, mediaType: "a" },
      layers: [],
    });
    const childDigest = await putBlob("multi", childBody);
    // index.json only lists the manifest list, not the child
    await putIndex("multi", [
      {
        digest: "sha256:" + "ee".repeat(32),
        mediaType: "application/vnd.oci.image.index.v1+json",
        size: 100,
        tag: "latest",
      },
    ]);

    const res = await zot.getManifest("multi", childDigest);
    expect("stream" in res).toBe(true);
    if (!("stream" in res)) return;
    expect(res.digest).toBe(childDigest);
    expect(res.contentType).toBe("application/vnd.docker.distribution.manifest.v2+json");
    await new Response(res.stream).text();
  });

  test("manifestExists resolves tag and returns metadata", async () => {
    const body = JSON.stringify({ schemaVersion: 2 });
    const digest = await putBlob("x", body);
    await putIndex("x", [{ digest, mediaType: "application/vnd.oci.image.manifest.v1+json", size: body.length, tag: "v1" }]);

    const res = await zot.manifestExists("x", "v1");
    expect(res).toEqual({
      exists: true,
      digest,
      contentType: "application/vnd.oci.image.manifest.v1+json",
      size: body.length,
    });
  });

  test("manifestExists returns false when tag absent", async () => {
    await putIndex("x", []);
    const res = await zot.manifestExists("x", "ghost");
    expect(res).toEqual({ exists: false });
  });

  test("listTags returns all annotated tags from index.json", async () => {
    const body = JSON.stringify({ schemaVersion: 2 });
    const d1 = await putBlob("many", body + "1");
    const d2 = await putBlob("many", body + "2");
    const d3 = await putBlob("many", body + "3");
    await putIndex("many", [
      { digest: d1, mediaType: "application/vnd.oci.image.manifest.v1+json", size: 10, tag: "v1" },
      { digest: d2, mediaType: "application/vnd.oci.image.manifest.v1+json", size: 10, tag: "v2" },
      { digest: d3, mediaType: "application/vnd.oci.image.manifest.v1+json", size: 10, tag: "latest" },
    ]);

    const res = await zot.listTags("many", 50);
    if (!("tags" in res)) throw new Error("expected tags");
    expect(res.name).toBe("many");
    expect(res.tags.sort()).toEqual(["latest", "v1", "v2"]);
    expect(res.truncated).toBe(false);
  });

  test("listTags returns empty list for a repo with no index.json", async () => {
    const res = await zot.listTags("doesnotexist", 10);
    if (!("tags" in res)) throw new Error("expected tags");
    expect(res.tags).toEqual([]);
  });

  test("listRepositories enumerates repos via oci-layout markers", async () => {
    await putIndex("alpine", []);
    await putIndex("ubuntu", []);
    await putIndex("busybox", []);

    const res = await zot.listRepositories();
    expect("repositories" in res).toBe(true);
    if (!("repositories" in res)) return;
    expect(res.repositories.sort()).toEqual(["alpine", "busybox", "ubuntu"]);
  });

  test("listRepositories ignores unrelated keys under the root prefix", async () => {
    await putIndex("good", []);
    // Some stray junk that should not be confused for a repo
    await env.REGISTRY.put(`${ROOT}/good/blobs/sha256/deadbeef`, "x");
    await env.REGISTRY.put(`${ROOT}/good/index.json`, "{}"); // already there but explicit

    const res = await zot.listRepositories();
    if (!("repositories" in res)) throw new Error("expected list");
    expect(res.repositories).toEqual(["good"]);
  });

  test("write operations all return 501 Not Implemented", async () => {
    const stream = new Response("").body!;
    const put = await zot.putManifest("r", "t", stream, { contentType: "x" });
    expect("response" in put && put.response.status).toBe(501);

    const start = await zot.startUpload("r");
    expect("response" in start && start.response.status).toBe(501);

    const mono = await zot.monolithicUpload("r", "sha256:aa", stream);
    expect(mono !== false && "response" in mono && mono.response.status).toBe(501);

    const gc = await zot.garbageCollection("r", "unreferenced" as any);
    expect(gc).toBe(false);
  });

  test("ZOT_ROOT_PREFIX env var overrides the default root", async () => {
    const customEnv = { ...env, ZOT_ROOT_PREFIX: "custom/base" } as Env;
    const custom = new ZotR2Registry(customEnv);

    const body = "blob-at-custom-root";
    const bytes = new TextEncoder().encode(body);
    const hex = await sha256Hex(bytes);
    await env.REGISTRY.put(`custom/base/myrepo/blobs/sha256/${hex}`, bytes);

    const res = await custom.getLayer("myrepo", `sha256:${hex}`);
    expect("stream" in res).toBe(true);
    if (!("stream" in res)) return;
    expect(await new Response(res.stream).text()).toBe(body);

    // Sanity: same digest under default root should NOT be found
    const noRes = await zot.getLayer("myrepo", `sha256:${hex}`);
    expect("response" in noRes).toBe(true);
  });
});
