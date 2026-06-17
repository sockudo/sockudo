import js from "@eslint/js";
import importPlugin from "eslint-plugin-import";
import tseslint from "typescript-eslint";

export default tseslint.config(
  {
    ignores: [
      "dist/**",
      "node_modules/**",
      "coverage/**",
      ".vite/**",
      "bench/.generated/**",
      "demo/**",
      "demo/node_modules/**",
      "docs/api/reference/**",
      "etc/*.d.ts",
    ],
  },
  js.configs.recommended,
  {
    files: ["**/*.ts", "**/*.tsx"],
    extends: [...tseslint.configs.strictTypeChecked, ...tseslint.configs.stylisticTypeChecked],
    languageOptions: {
      parserOptions: {
        project: "./tsconfig.json",
        tsconfigRootDir: import.meta.dirname,
      },
    },
    plugins: {
      import: importPlugin,
    },
    settings: {
      "import/resolver": {
        typescript: {
          alwaysTryTypes: true,
          project: "./tsconfig.json",
        },
      },
    },
    rules: {
      "import/extensions": [
        "error",
        "ignorePackages",
        {
          js: "always",
          jsx: "always",
          mjs: "always",
          ts: "never",
          tsx: "never",
        },
      ],
      "import/no-restricted-paths": [
        "error",
        {
          zones: [
            {
              target: "./src/core",
              from: ["./src/react", "./src/vercel"],
              message: "Core transport code must not import React or Vercel entry layers.",
            },
            {
              target: "./src/realtime",
              from: "./src/core/transport",
              message: "The realtime adapter must stay below core transport logic.",
            },
          ],
        },
      ],
      "no-restricted-imports": [
        "error",
        {
          paths: [
            {
              name: "@sockudo/client",
              message: "Only src/realtime may import the @sockudo/client peer dependency.",
            },
          ],
        },
      ],
      "@typescript-eslint/consistent-type-imports": [
        "error",
        {
          fixStyle: "inline-type-imports",
        },
      ],
      "@typescript-eslint/no-explicit-any": "error",
    },
  },
  {
    files: ["src/realtime/**/*.ts", "src/realtime/**/*.tsx"],
    rules: {
      "no-restricted-imports": "off",
    },
  },
  {
    files: ["scripts/**/*.mjs", "eslint.config.mjs"],
    languageOptions: {
      globals: {
        console: "readonly",
        process: "readonly",
      },
    },
    rules: {
      "import/extensions": "off",
    },
  },
  {
    files: ["*.cjs"],
    languageOptions: {
      globals: {
        module: "readonly",
      },
      sourceType: "commonjs",
    },
  },
);
