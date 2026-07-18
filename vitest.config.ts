import { defineConfig } from "vitest/config";
import path from "path";

const workspaceRoot = path.resolve(__dirname, "..");

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["src/**/*.test.ts", "src/**/*.spec.ts"],
    coverage: {
      provider: "v8",
      reporter: ["text", "lcov"],
    },
  },
  resolve: {
    alias: {
      "@agentix-e/db-cluster": path.resolve(workspaceRoot, "db-cluster", "src"),
      "@agentix-e/db-federation": path.resolve(__dirname, "src"),
      "@agentix-e/db-operations": path.resolve(workspaceRoot, "db-operations", "packages"),
    },
  },
});
