const { chromium } = require("playwright");
const fs = require("fs");
const path = require("path");

const lectureRoot = path.resolve(__dirname, "..", "..", "..", "Lecture");
const baseUrl = process.env.LECTURE_BASE_URL || "http://127.0.0.1:8765";
const screenshotPath = path.resolve(
  __dirname,
  "..",
  "test-results",
  "lecture-lab03-chinese-first.png",
);
const termTooltipScreenshotPath = path.resolve(
  __dirname,
  "..",
  "test-results",
  "lecture-capability-tooltip.png",
);

function findLecturePages(directory, pages = []) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const fullPath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      findLecturePages(fullPath, pages);
    } else if (entry.name === "index.html") {
      pages.push(fullPath);
    }
  }
  return pages;
}

async function validatePage(context, filePath, viewportName) {
  const relativePath = path.relative(lectureRoot, filePath).split(path.sep).join("/");
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", (error) => errors.push(`pageerror:${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(`console:${message.text()}`);
  });

  const response = await page.goto(`${baseUrl}/${relativePath}`, {
    waitUntil: "networkidle",
    timeout: 20_000,
  });
  await page.waitForTimeout(300);

  const state = await page.evaluate(() => ({
    overflow: document.documentElement.scrollWidth - window.innerWidth,
    mermaidSources: document.querySelectorAll("pre.mermaid").length,
    mermaidSvgs: document.querySelectorAll(".mermaid-panel svg").length,
    mermaidFailures: document.querySelectorAll(".mermaid-failed").length,
    termHelpTriggers: document.querySelectorAll(".term-help").length,
  }));

  if (!response?.ok()) errors.push(`http:${response?.status()}`);
  if (state.overflow > 1) errors.push(`overflow:${state.overflow}`);
  if (state.mermaidFailures) {
    errors.push(`mermaid-failures:${state.mermaidFailures}`);
  }
  if (state.mermaidSources && !state.mermaidSvgs) {
    errors.push("mermaid-not-rendered");
  }

  let termTooltipChecked = false;
  if (relativePath === "Module_05_Strategy_Semantics/index.html") {
    const trigger = page.locator('.term-help[data-term-key="capability-verdict"]');
    const tooltip = page.locator("#lecture-term-tooltip");
    if ((await trigger.count()) !== 1) {
      errors.push("capability-tooltip-trigger-missing");
    } else {
      if (viewportName === "desktop") await trigger.hover();
      else await trigger.click();
      await tooltip.waitFor({ state: "visible", timeout: 2_000 });
      const tooltipState = await tooltip.evaluate((element) => {
        const box = element.getBoundingClientRect();
        return {
          text: element.textContent,
          inViewport:
            box.left >= 0 &&
            box.top >= 0 &&
            box.right <= window.innerWidth &&
            box.bottom <= window.innerHeight,
        };
      });
      if (!tooltipState.text.includes("它不是只檢查預設指標")) {
        errors.push("capability-tooltip-definition-missing");
      }
      if (!tooltipState.inViewport) errors.push("term-tooltip-outside-viewport");
      termTooltipChecked = true;
      if (viewportName === "desktop") {
        fs.mkdirSync(path.dirname(termTooltipScreenshotPath), { recursive: true });
        await page.screenshot({ path: termTooltipScreenshotPath, fullPage: false });
      }
    }
  }

  if (viewportName === "desktop" && relativePath === "Lab_03_WFA_Rolling/index.html") {
    fs.mkdirSync(path.dirname(screenshotPath), { recursive: true });
    await page.screenshot({ path: screenshotPath, fullPage: true });
  }

  await page.close();
  return { viewport: viewportName, relativePath, termTooltipChecked, ...state, errors };
}

async function main() {
  const pages = findLecturePages(lectureRoot).sort();
  const browser = await chromium.launch({ headless: true });
  const results = [];

  for (const viewport of [
    { name: "desktop", width: 1440, height: 900 },
    { name: "mobile", width: 390, height: 844 },
  ]) {
    const context = await browser.newContext({
      viewport: { width: viewport.width, height: viewport.height },
    });
    for (const page of pages) {
      results.push(await validatePage(context, page, viewport.name));
    }
    await context.close();
  }

  await browser.close();
  const failures = results.filter((result) => result.errors.length);
  const pagesWithTermHelp = new Set(
    results.filter((result) => result.termHelpTriggers).map((result) => result.relativePath),
  );
  const allRelativePaths = pages.map((filePath) =>
    path.relative(lectureRoot, filePath).split(path.sep).join("/"),
  );
  const summary = {
    pages: pages.length,
    checks: results.length,
    mermaidChecks: results.filter((result) => result.mermaidSources).length,
    termTooltipChecks: results.filter((result) => result.termTooltipChecked).length,
    pagesWithTermHelp: pagesWithTermHelp.size,
    missingTermHelpPages: allRelativePaths.filter((page) => !pagesWithTermHelp.has(page)),
    maxOverflow: Math.max(...results.map((result) => result.overflow)),
    failures,
    screenshotPath,
    termTooltipScreenshotPath,
  };
  console.log(JSON.stringify(summary, null, 2));
  process.exitCode = failures.length ? 1 : 0;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
