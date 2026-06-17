import { expect, test } from "@playwright/test";

test("renders the useChat quickstart shell", async ({ page }) => {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Vercel useChat quickstart" }),
  ).toBeVisible();
  await expect(page.getByRole("textbox")).toContainText(
    "Say hello over Sockudo AI Transport.",
  );
  await expect(page.getByRole("button", { name: "Send" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Stop" })).toBeVisible();
});
