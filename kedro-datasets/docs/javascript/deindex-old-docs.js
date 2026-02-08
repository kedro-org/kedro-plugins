if (document.querySelector("meta[name='readthedocs-addons-api-version']") === null) {
    const meta = document.createElement("meta");
    meta.name = "readthedocs-addons-api-version";
    meta.content = "1";
    document.head.appendChild(meta);
}

function ensureNoIndexMeta() {
    if (!document.querySelector('meta[name="robots"]')) {
        const meta = document.createElement("meta");
        meta.name = "robots";
        meta.content = "noindex, nofollow";
        document.head.appendChild(meta);
    }
}

function handleReadTheDocsData(data) {
    const versionSlug = data.versions.current.slug;

    console.log("Kedro-datasets version:", versionSlug);

    // Only the latest release should be indexed
    if (versionSlug !== "kedro-datasets-9.1.1") {
        ensureNoIndexMeta();
    }
}

if (window.ReadTheDocsEventData !== undefined) {
    handleReadTheDocsData(window.ReadTheDocsEventData.data());
}

// Listen for future RTD Addons events (SPA navigation, version switch)
document.addEventListener("readthedocs-addons-data-ready", function (event) {
    handleReadTheDocsData(event.detail.data());
});

// Fallback: if RTD Addons are unavailable, default to noindex
ensureNoIndexMeta();
