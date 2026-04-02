(() => {
  const host = window.location.hostname || "127.0.0.1";
  const isLocal = host === "127.0.0.1" || host === "localhost";

  window.QUBO_APP_CONFIG = {
    apiBaseUrl: isLocal ? "http://127.0.0.1:8020" : "",
  };
})();
