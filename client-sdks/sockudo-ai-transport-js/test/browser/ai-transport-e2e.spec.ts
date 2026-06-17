import { expect, type Page, test } from "@playwright/test";

interface HarnessState {
  ready: boolean;
  channelName: string;
  normal?: {
    chunks: string[];
    primaryText: string;
    mirrorText: string;
  };
  late?: {
    text: string;
    count: number;
  };
  cancel?: {
    chunks: string[];
    turnId: string;
    ended: boolean;
    cancelEvent: boolean;
    reason?: string;
  };
  error?: string;
}

test("streams, mirrors, cancels, and replays history in the browser", async ({
  page,
}, testInfo) => {
  const project = testInfo.project.name.replace(/[^a-z0-9-]/giu, "-");
  const channel = `private-ai-e2e-${String(Date.now())}-${project}-${String(testInfo.workerIndex)}`;
  await page.goto(`/e2e?channel=${encodeURIComponent(channel)}`);
  await expect(page.getByTestId("ready")).toHaveText("ready");

  await page.getByTestId("run-normal").click();
  await expect
    .poll(async () => (await readState(page)).normal?.mirrorText ?? "")
    .toContain("Echo: browser normal");
  const normal = (await readState(page)).normal;
  expect(normal?.primaryText).toContain("Echo: browser normal");
  expect(normal?.chunks).toEqual(
    expect.arrayContaining([
      "start",
      "text-start",
      "text-delta",
      "text-end",
      "finish",
    ]),
  );

  await page.getByTestId("load-late").click();
  await expect
    .poll(async () => (await readState(page)).late?.text ?? "")
    .toContain("Echo: browser normal");
  expect((await readState(page)).late?.count).toBeGreaterThanOrEqual(1);

  await page.getByTestId("run-cancel").click();
  await expect
    .poll(async () => (await readState(page)).cancel?.ended)
    .toBe(true);
  const cancel = (await readState(page)).cancel;
  expect(cancel?.turnId).toMatch(/^turn[_-]/u);
  expect(cancel?.cancelEvent).toBe(true);
  expect(cancel?.reason).toBe("cancelled");
  expect(cancel?.chunks.length).toBeGreaterThan(0);
});

async function readState(page: Page): Promise<HarnessState> {
  const text = await page.getByTestId("state").textContent();
  const state = JSON.parse(text ?? "{}") as HarnessState;
  if (state.error) {
    throw new Error(state.error);
  }
  return state;
}
