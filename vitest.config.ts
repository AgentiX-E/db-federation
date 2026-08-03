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
      include: ["src/**/*.ts"],
      exclude: [
        "src/**/*.test.ts",
        "src/**/*.spec.ts",
        "src/**/__tests__/**",
        "src/index.ts",
        "src/index.test.ts",
        "src/interfaces/**",
        "src/transport/TcpSecureTransport.ts",
      ],
      reporter: ["text", "json-summary", "lcov"],
      thresholds: {
        lines: 92,
        branches: 88,
        functions: 95,
        statements: 92,
      },
    },
  },
  resolve: {
    alias: {
      "@agentix-e/db-cluster": path.resolve(workspaceRoot, "db-cluster", "src"),
      "@agentix-e/db-federation": path.resolve(__dirname, "src"),
      "@agentix-e/db-operations": path.resolve(workspaceRoot, "db-operations", "packages"),
      "@agentix-e/db-operations-chaosengine": path.resolve(
        workspaceRoot, "db-operations", "packages", "chaosengine", "src",
      ),
    },
  },
});
