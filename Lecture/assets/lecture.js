const courseSections = [
  {
    label: "開始",
    pages: [
      { title: "首頁", href: "index.html", kind: "課程首頁（Course Home）", minutes: 4 },
    ],
  },
  {
    label: "實作（Labs）",
    pages: [
      { title: "實驗（Lab）01 跑一次回測", href: "Lab_01_Run_A_Backtest/index.html", kind: "實驗（Lab）", minutes: 12 },
      { title: "實驗（Lab）02 參數矩陣", href: "Lab_02_Parameter_Matrix/index.html", kind: "實驗（Lab）", minutes: 12 },
      { title: "實驗（Lab）03 前向分析（WFA）", href: "Lab_03_WFA_Rolling/index.html", kind: "實驗（Lab）", minutes: 14 },
      { title: "實驗（Lab）04 資料來源檢查", href: "Lab_04_Provider_Check/index.html", kind: "實驗（Lab）", minutes: 10 },
      { title: "實驗（Lab）05 績效分析", href: "Lab_05_Performance_Analysis/index.html", kind: "實驗（Lab）", minutes: 12 },
    ],
  },
  {
    label: "基礎 00-03",
    pages: [
      { title: "00 入門與人工智能手冊（AI Manual）", href: "Module_00_Getting_Started/index.html", kind: "基礎（Foundation）", minutes: 6 },
      { title: "01 系統地圖", href: "Module_01_System_Map/index.html", kind: "架構（Architecture）", minutes: 8 },
      { title: "02 資料來源與資料邊界", href: "Module_02_Data_Providers/index.html", kind: "資料（Data）", minutes: 8 },
      { title: "03 策略執行設定（Strategy Run Config）", href: "Module_03_Strategy_Run_Config/index.html", kind: "契約（Contract）", minutes: 10 },
    ],
  },
  {
    label: "引擎 04-06",
    pages: [
      { title: "04 統一回測引擎", href: "Module_04_Backtest_Basics/index.html", kind: "引擎（Engine）", minutes: 9 },
      { title: "05 策略組件（Building Blocks）", href: "Module_05_Strategy_Semantics/index.html", kind: "策略（Strategy）", minutes: 10 },
      { title: "06 參數矩陣（Parameter Matrix）", href: "Module_06_Parameter_Matrix/index.html", kind: "研究（Research）", minutes: 8 },
    ],
  },
  {
    label: "證據 07-11",
    pages: [
      { title: "07 回測報告閱讀", href: "Module_07_Backtests_Report/index.html", kind: "證據（Evidence）", minutes: 12 },
      { title: "08 前向分析（WFA）", href: "Module_08_WFA_Rolling_Validation/index.html", kind: "驗證（Validation）", minutes: 10 },
      { title: "09 會計與風險（Accounting / Risk）", href: "Module_09_Accounting_Risk_Invariants/index.html", kind: "風險（Risk）", minutes: 10 },
      { title: "10 資料來源擴充", href: "Module_10_Data_Provider_Extension/index.html", kind: "擴充（Extension）", minutes: 8 },
      { title: "11 可選因子診斷", href: "Module_11_Factor_Analysis_Preview/index.html", kind: "選修（Optional）", minutes: 7 },
    ],
  },
  {
    label: "參考",
    pages: [
      { title: "附錄（Appendix）", href: "Appendix/index.html", kind: "參考（Reference）", minutes: 8 },
    ],
  },
];

const pages = courseSections.flatMap((section) => section.pages);

const lectureGlossaryTerms = [
  {
    key: "capability-verdict",
    label: "能力判斷（capability verdict）",
    aliases: ["能力判斷（capability verdict）", "能力判斷（Verdict）", "能力判斷"],
    definition: "平台會核對資料來源、指標、訊號、選股、配置、成交、風控及驗證規則，判斷策略可建立、需要補充資料，或需要開發新組件。它不是只檢查預設指標。",
  },
  {
    key: "strategy-run-config",
    label: "策略執行設定檔（strategy_run config）",
    aliases: ["策略執行設定檔（strategy_run config）", "設定檔（config）"],
    definition: "一份可驗證的策略說明，記錄市場資料、訊號、配置、成交、成本、風控及參數範圍。人工智能與執行引擎以同一份內容溝通。",
  },
  {
    key: "run-center",
    label: "執行中心（Run Center）",
    aliases: ["執行中心（Run Center）"],
    definition: "用來選擇設定檔與啟動工作，也會顯示進度及錯誤。它負責安排工作，不負責計算回測結果。",
  },
  {
    key: "parameter-matrix",
    label: "參數矩陣（Parameter Matrix）",
    aliases: ["參數矩陣（Parameter Matrix）", "參數矩陣（Matrix）"],
    definition: "把指定參數範圍展開成多個候選組合，再用同一套資料與回測規則逐一計算。它是候選測試，不等於前向分析。",
  },
  {
    key: "wfa",
    label: "前向分析（WFA）",
    aliases: ["前向分析（WFA）"],
    definition: "按時間切開樣本內與樣本外資料。每個視窗只用較早資料選參數，再用之後的資料檢查結果，減少用未來資料挑選策略的風險。",
  },
  {
    key: "rolling-validation",
    label: "滾動驗證（rolling validation）",
    aliases: ["滾動驗證（rolling validation）", "滾動驗證（Rolling validation）"],
    definition: "把驗證視窗沿時間移動，觀察同一套規則在多段市場期間的表現。它可顯示穩定性，但不能保證未來報酬。",
  },
  {
    key: "in-sample",
    label: "樣本內（IS）",
    aliases: ["樣本內（IS）", "樣本內視窗（IS window）"],
    definition: "用來比較候選參數及選出方案的較早一段資料。這段結果參與選擇，因此不能當成未見資料的驗證。",
  },
  {
    key: "out-of-sample",
    label: "樣本外（OOS）",
    aliases: ["樣本外（OOS）", "樣本外視窗（OOS window）"],
    definition: "選定參數後才用來檢查的下一段資料。它較接近未知資料測試，但仍屬歷史回測。",
  },
  {
    key: "window-count",
    label: "視窗數量（window count）",
    aliases: ["視窗數量（window count）"],
    definition: "前向分析共建立多少組樣本內與樣本外時間區段。視窗太少時，穩定性判斷的證據會較弱。",
  },
  {
    key: "objective",
    label: "目標函數（objective）",
    aliases: ["目標函數（objective）"],
    definition: "參數搜尋希望提高或降低的計算目標，例如提高風險調整後報酬。它決定搜尋方向，不代表最後選擇只看一個數字。",
  },
  {
    key: "selection-metric",
    label: "選擇指標（selection metric）",
    aliases: ["選擇指標（selection metric）"],
    definition: "用來比較候選方案及選出視窗最優解的指標。讀者要核對它與策略目標、風險限制及成本假設是否一致。",
  },
  {
    key: "selected-optimum",
    label: "選定最優解（selected optimum）",
    aliases: ["選定最優解（selected optimum）"],
    definition: "在該樣本內視窗按選擇規則勝出的參數組合。它只代表該次選擇結果，不代表全市場或未來的最佳答案。",
  },
  {
    key: "sharpe",
    label: "夏普比率（Sharpe）",
    aliases: ["夏普比率（Sharpe）", "樣本外夏普比率（OOS Sharpe）"],
    definition: "用超額報酬除以報酬波動，衡量每承受一單位波動得到多少報酬。數值受年化方式、無風險利率及樣本長度影響。",
  },
  {
    key: "calmar",
    label: "卡瑪比率（Calmar）",
    aliases: ["卡瑪比率（Calmar）", "樣本外卡瑪比率（OOS Calmar）"],
    definition: "用年化成長率除以最大回撤，衡量報酬與最深資金跌幅的關係。最大回撤接近零時要小心比率失真。",
  },
  {
    key: "sortino",
    label: "索提諾比率（Sortino）",
    aliases: ["索提諾比率（Sortino）"],
    definition: "只把低於目標的報酬波動視為風險，用來區分下行波動與上行波動。結果取決於目標報酬及年化設定。",
  },
  {
    key: "cagr",
    label: "年化複合成長率（CAGR）",
    aliases: ["年化複合成長率（CAGR）"],
    definition: "把起始至結束的總成長換算成每年複合成長率。它描述整段速度，不顯示中途回撤或報酬路徑。",
  },
  {
    key: "total-return",
    label: "總報酬（Total Return）",
    aliases: ["總報酬（Total Return）", "樣本外總報酬（OOS total return）"],
    definition: "期末資金相對期初資金的總變化，已扣除哪些成本要按結果契約確認。它不反映持有年期或中途風險。",
  },
  {
    key: "max-drawdown",
    label: "最大回撤（Max Drawdown / MDD）",
    aliases: ["最大回撤（Max Drawdown / MDD）", "最大回撤（max drawdown）"],
    definition: "資金曲線由歷史高點跌至其後低點的最大幅度。它顯示最深跌幅，但不說明回撤持續多久。",
  },
  {
    key: "oos-is-ratio",
    label: "樣本外／樣本內比率（OOS / IS ratio）",
    aliases: ["樣本外／樣本內比率（OOS / IS ratio）", "樣本外／樣本內比率（OOS/IS ratio）"],
    definition: "把樣本外表現與樣本內表現相比，觀察選定方案離開訓練區段後保留多少效果。比率要連同原始指標及負值情況閱讀。",
  },
  {
    key: "allocation-by-window",
    label: "各視窗配置（allocation by window）",
    aliases: ["各視窗配置（allocation by window）"],
    definition: "列出每個驗證視窗選出的資產與目標權重，用來檢查投資組合是否頻繁換方向或集中於少數資產。",
  },
  {
    key: "asset-contribution",
    label: "資產貢獻（asset contribution）",
    aliases: ["資產貢獻（asset contribution）", "資產貢獻（Asset Contribution）"],
    definition: "估算各資產為投資組合報酬帶來的部分。它可揭示報酬是否由少數資產主導，合計方式要按歸因契約確認。",
  },
  {
    key: "turnover",
    label: "換手率（turnover）",
    aliases: ["換手率（turnover）", "平均交易換手率（Avg Trade Turnover）"],
    definition: "衡量持倉在一段時間內被買入、賣出或重新配置的幅度。費率與滑價不為零時，換手率上升會增加成本。",
  },
  {
    key: "cost-drag",
    label: "成本拖累（cost drag）",
    aliases: ["成本拖累（cost drag）", "成本拖累／交易成本欄位（Cost Drag / trade cost fields）"],
    definition: "交易費、滑價及其他執行成本令策略報酬減少的部分。它應由成交與成本記錄計算，不應由前端估算。",
  },
  {
    key: "risk-gate",
    label: "風控門檻（risk gate）",
    aliases: ["風控門檻（risk gate）", "風控門檻（Risk Gate）", "風控門檻事件數（risk gate event counts）"],
    definition: "當回撤、曝險或其他受監察數值越過設定門檻時觸發的控制規則。事件記錄要包含時間、觀察值、門檻、行動及結果。",
  },
  {
    key: "benchmark",
    label: "比較基準（benchmark）",
    aliases: ["比較基準（benchmark）"],
    definition: "用來回答策略是否勝過一個可比較的被動方案。基準資產和日期範圍要與策略對齊，成本及資料頻率也要一致。",
  },
  {
    key: "equity-curve",
    label: "資金曲線（equity curve）",
    aliases: ["資金曲線（equity curve）", "資金曲線（equity）"],
    definition: "按時間記錄投資組合總資產價值的序列，包括現金與持倉市值。報酬、回撤及多項風險指標都由這條序列產生。",
  },
  {
    key: "slippage",
    label: "滑價（slippage）",
    aliases: ["滑價（slippage）", "滑價"],
    definition: "訊號參考價格與模擬成交價格之間的差距。設定為零代表沒有模擬這項摩擦，不代表真實交易沒有滑價。",
  },
  {
    key: "rebalance",
    label: "再平衡（rebalance）",
    aliases: ["再平衡（rebalance）", "有效再平衡（Active Rebalances）"],
    definition: "把目前持倉調整至新目標權重的事件。只有目標或持倉產生實際變化時，才算有效再平衡。",
  },
  {
    key: "parameter-domain",
    label: "參數範圍（parameter domain）",
    aliases: ["參數範圍（parameter domain）", "參數範圍（Parameter Domain）"],
    definition: "列出參數可採用的值，以及範圍起點、終點和間距。參數矩陣與前向尋優按這個範圍建立候選組合。",
  },
  {
    key: "data-health",
    label: "資料健康（Data Health）",
    aliases: ["資料健康（Data Health）", "資料健康（data health）"],
    definition: "記錄請求資產、已載入資產、缺失資產、有效起始日及資料問題。它用來判斷回測是否取得預期市場資料。",
  },
  {
    key: "target-weights",
    label: "目標權重（target weights）",
    aliases: ["目標權重（target weights）", "目標權重（Target Weight）"],
    definition: "策略在某個時間點希望各資產佔投資組合的比例。引擎會根據目前持倉、現金及成本計算需要的交易。",
  },
  {
    key: "canonical-result-bundle",
    label: "標準結果套件（CanonicalResultBundle）",
    aliases: ["標準結果套件（CanonicalResultBundle）"],
    definition: "Rust 引擎交出的統一結果，包括資金曲線、成交、持倉、配置、成本及風控事件。驗證、指標與前端都應讀取這份結果。",
  },
  {
    key: "invariant",
    label: "不變條件（invariant）",
    aliases: ["不變條件（invariant）", "不變條件（Invariant）"],
    definition: "每次回測都必須成立的記帳或風控規則，例如現金、持倉與資產總值要互相吻合。違反時結果不可交給前端。",
  },
  {
    key: "golden-test",
    label: "黃金測試（golden test）",
    aliases: ["黃金測試（golden test）", "黃金與對照測試（golden/oracle tests）"],
    definition: "把目前結果與已核准的固定輸出比較，用來發現重構造成的行為變化。測試通過代表與基準一致，不代表策略有投資價值。",
  },
  {
    key: "provider",
    label: "資料供應者（provider）",
    aliases: ["資料供應者（provider）", "資料來源（provider）"],
    definition: "負責讀取外部或本機市場資料，再轉成平台統一欄位的組件。回測引擎不應包含來源專用規則。",
  },
  {
    key: "universe",
    label: "資產範圍（universe）",
    aliases: ["資產範圍（universe）"],
    definition: "策略在該次執行中可以觀察或交易的資產集合。它不等於最後持倉，也不保證每項資產都有完整資料。",
  },
  {
    key: "factor-diagnostics",
    label: "因子診斷（factor diagnostics）",
    aliases: ["因子診斷（factor diagnostics）", "可選因子診斷"],
    definition: "用統計方法檢查因子資料的分布、關係及研究訊號。它是使用者選擇的研究工具，不是每次回測或前端報告的必經步驟。",
  },
  {
    key: "diagnostic-payload",
    label: "診斷資料（diagnostic payload）",
    aliases: ["診斷資料（diagnostic payload）"],
    definition: "因子或統計研究工具交給介面的結構化資料。沒有執行診斷時，這份資料可以不存在，普通回測仍可完成。",
  },
];

function basePrefix() {
  const path = location.pathname.replaceAll("\\", "/");
  return /\/(Module_|Lab_|Appendix\/)/.test(path) ? "../" : "";
}

function currentLecturePath() {
  const path = decodeURIComponent(location.pathname.replaceAll("\\", "/"));
  const marker = "/Lecture/";
  const position = path.lastIndexOf(marker);
  return position >= 0 ? path.slice(position + marker.length) : path.split("/").slice(-2).join("/");
}

function isCurrentPage(href) {
  const current = currentLecturePath().replace(/^\/+/, "");
  return current === href || (href === "index.html" && current === "");
}

function escapeRegularExpression(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildTermTooltip() {
  const tooltip = document.createElement("aside");
  tooltip.id = "lecture-term-tooltip";
  tooltip.className = "term-tooltip";
  tooltip.setAttribute("role", "tooltip");
  tooltip.hidden = true;
  tooltip.innerHTML = `
    <strong data-term-title></strong>
    <span data-term-definition></span>
  `;
  document.body.appendChild(tooltip);
  return tooltip;
}

function enhanceTermHelp() {
  const article = document.querySelector("article");
  if (!article || !lectureGlossaryTerms.length) return;

  const aliasToTerm = new Map();
  lectureGlossaryTerms.forEach((term) => {
    term.aliases.forEach((alias) => aliasToTerm.set(alias, term));
  });
  const aliases = [...aliasToTerm.keys()].sort((left, right) => right.length - left.length);
  const matcher = new RegExp(`(${aliases.map(escapeRegularExpression).join("|")})`, "g");
  const seenTerms = new Set();
  const textNodes = [];
  const blockedSelector = "code, pre, script, style, a, button, input, textarea, select, .mermaid, .term-help";
  const walker = document.createTreeWalker(article, NodeFilter.SHOW_TEXT, {
    acceptNode(node) {
      if (!node.nodeValue?.trim() || node.parentElement?.closest(blockedSelector)) {
        return NodeFilter.FILTER_REJECT;
      }
      return aliases.some((alias) => node.nodeValue.includes(alias))
        ? NodeFilter.FILTER_ACCEPT
        : NodeFilter.FILTER_REJECT;
    },
  });

  while (walker.nextNode()) textNodes.push(walker.currentNode);

  textNodes.forEach((node) => {
    const parts = node.nodeValue.split(matcher);
    if (parts.length === 1) return;
    const fragment = document.createDocumentFragment();
    parts.forEach((part) => {
      const term = aliasToTerm.get(part);
      if (!term || seenTerms.has(term.key)) {
        fragment.appendChild(document.createTextNode(part));
        return;
      }
      seenTerms.add(term.key);
      const trigger = document.createElement("span");
      trigger.className = "term-help";
      trigger.tabIndex = 0;
      trigger.setAttribute("role", "button");
      trigger.setAttribute("aria-describedby", "lecture-term-tooltip");
      trigger.setAttribute("aria-label", `${term.label}。${term.definition}`);
      trigger.dataset.termKey = term.key;
      trigger.append(document.createTextNode(part));
      const marker = document.createElement("span");
      marker.className = "term-help-marker";
      marker.setAttribute("aria-hidden", "true");
      marker.textContent = "?";
      trigger.appendChild(marker);
      fragment.appendChild(trigger);
    });
    node.replaceWith(fragment);
  });

  const triggers = [...article.querySelectorAll(".term-help")];
  if (!triggers.length) return;
  const tooltip = buildTermTooltip();
  let activeTrigger = null;
  let pinnedTrigger = null;

  const positionTooltip = (trigger) => {
    const triggerBox = trigger.getBoundingClientRect();
    const tooltipBox = tooltip.getBoundingClientRect();
    const edge = 12;
    const gap = 10;
    const left = Math.min(
      window.innerWidth - tooltipBox.width - edge,
      Math.max(edge, triggerBox.left + (triggerBox.width - tooltipBox.width) / 2),
    );
    const fitsBelow = triggerBox.bottom + gap + tooltipBox.height <= window.innerHeight - edge;
    const top = fitsBelow
      ? triggerBox.bottom + gap
      : Math.max(edge, triggerBox.top - tooltipBox.height - gap);
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
  };

  const showTooltip = (trigger) => {
    const term = lectureGlossaryTerms.find((item) => item.key === trigger.dataset.termKey);
    if (!term) return;
    activeTrigger?.classList.remove("term-help-active");
    activeTrigger = trigger;
    activeTrigger.classList.add("term-help-active");
    tooltip.querySelector("[data-term-title]").textContent = term.label;
    tooltip.querySelector("[data-term-definition]").textContent = term.definition;
    tooltip.hidden = false;
    positionTooltip(trigger);
  };

  const hideTooltip = () => {
    if (pinnedTrigger) return;
    activeTrigger?.classList.remove("term-help-active");
    activeTrigger = null;
    tooltip.hidden = true;
  };

  const closePinnedTooltip = () => {
    pinnedTrigger = null;
    hideTooltip();
  };

  triggers.forEach((trigger) => {
    trigger.addEventListener("mouseenter", () => showTooltip(trigger));
    trigger.addEventListener("mouseleave", hideTooltip);
    trigger.addEventListener("focus", () => showTooltip(trigger));
    trigger.addEventListener("blur", hideTooltip);
    trigger.addEventListener("click", (event) => {
      event.stopPropagation();
      if (pinnedTrigger === trigger) {
        closePinnedTooltip();
        return;
      }
      pinnedTrigger = trigger;
      showTooltip(trigger);
    });
    trigger.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        trigger.click();
      }
      if (event.key === "Escape") closePinnedTooltip();
    });
  });

  document.addEventListener("click", (event) => {
    if (pinnedTrigger && !event.target.closest(".term-help")) closePinnedTooltip();
  });
  window.addEventListener("resize", () => activeTrigger && positionTooltip(activeTrigger));
  window.addEventListener("scroll", () => activeTrigger && positionTooltip(activeTrigger), true);
}

function buildSidebar() {
  const nav = document.querySelector("[data-nav]");
  if (!nav) return;

  const prefix = basePrefix();
  nav.innerHTML = courseSections
    .map((section) => `
      <section class="nav-section">
        <p class="nav-section-label">${section.label}</p>
        ${section.pages.map((page) => `
          <a class="${isCurrentPage(page.href) ? "active" : ""}"
             href="${prefix + page.href}"
             data-title="${page.title.toLowerCase()}"
             ${isCurrentPage(page.href) ? 'aria-current="page"' : ""}>${page.title}</a>
        `).join("")}
      </section>
    `)
    .join("");
}

function buildMobileNavigation() {
  const sidebar = document.querySelector(".sidebar");
  if (!sidebar) return;

  const button = document.createElement("button");
  button.type = "button";
  button.className = "nav-toggle";
  button.setAttribute("aria-expanded", "false");
  button.innerHTML = '<span aria-hidden="true">課程地圖（Course map）</span><strong>章節</strong>';

  const backdrop = document.createElement("button");
  backdrop.type = "button";
  backdrop.className = "nav-backdrop";
  backdrop.setAttribute("aria-label", "關閉章節導覽");

  const setOpen = (open) => {
    document.body.classList.toggle("nav-open", open);
    button.setAttribute("aria-expanded", String(open));
  };

  button.addEventListener("click", () => setOpen(!document.body.classList.contains("nav-open")));
  backdrop.addEventListener("click", () => setOpen(false));
  sidebar.querySelectorAll("a").forEach((link) => link.addEventListener("click", () => setOpen(false)));
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") setOpen(false);
  });

  document.body.append(button, backdrop);
}

function enableSearch() {
  const input = document.querySelector("[data-search]");
  if (!input) return;

  input.addEventListener("input", () => {
    const query = input.value.trim().toLowerCase();
    document.querySelectorAll(".nav-section").forEach((section) => {
      const links = [...section.querySelectorAll("a")];
      links.forEach((link) => link.classList.toggle("hidden", Boolean(query) && !link.dataset.title.includes(query)));
      section.classList.toggle("hidden", links.every((link) => link.classList.contains("hidden")));
    });
  });
}

function buildLessonMeta() {
  const article = document.querySelector("article");
  if (!article || article.querySelector(".lesson-meta")) return;

  const currentIndex = pages.findIndex((page) => isCurrentPage(page.href));
  if (currentIndex < 0) return;
  const page = pages[currentIndex];
  const progressValue = Math.round(((currentIndex + 1) / pages.length) * 100);
  const meta = document.createElement("div");
  meta.className = "lesson-meta";
  meta.innerHTML = `
    <div class="lesson-meta-items">
      <span>${page.kind}</span>
      <span>約 ${page.minutes} 分鐘</span>
      <span>第 ${currentIndex + 1} / ${pages.length} 頁</span>
      <span class="contract-badge">現行契約（Current contract）</span>
    </div>
    <div class="progress-ring" style="--progress:${progressValue}" role="img" aria-label="課程進度 ${progressValue}%">
      <strong>${progressValue}%</strong>
    </div>
  `;

  const kicker = article.querySelector(".kicker");
  if (kicker) kicker.insertAdjacentElement("afterend", meta);
}

function buildCourseStepper() {
  const article = document.querySelector("article");
  const meta = article?.querySelector(".lesson-meta");
  const currentIndex = pages.findIndex((page) => isCurrentPage(page.href));
  if (!article || !meta || currentIndex < 0 || article.querySelector(".course-stepper")) return;

  const prefix = basePrefix();
  const start = Math.max(0, Math.min(currentIndex - 1, pages.length - 3));
  const visiblePages = pages.slice(start, start + 3);
  const stepper = document.createElement("nav");
  stepper.className = "course-stepper";
  stepper.setAttribute("aria-label", "目前課程進度");
  stepper.innerHTML = visiblePages.map((page, offset) => {
    const pageIndex = start + offset;
    const state = pageIndex < currentIndex ? "complete" : pageIndex === currentIndex ? "current" : "upcoming";
    return `
      <a class="${state}" href="${prefix + page.href}" ${state === "current" ? 'aria-current="page"' : ""}>
        <span>${String(pageIndex + 1).padStart(2, "0")}</span>
        <small>${page.title}</small>
      </a>
    `;
  }).join("");
  meta.insertAdjacentElement("afterend", stepper);
}

function buildProgress() {
  const currentIndex = pages.findIndex((page) => isCurrentPage(page.href));
  if (currentIndex < 0) return;
  const progress = document.createElement("div");
  progress.className = "course-progress";
  progress.setAttribute("aria-hidden", "true");
  progress.innerHTML = `<span style="width:${((currentIndex + 1) / pages.length) * 100}%"></span>`;
  document.body.appendChild(progress);
}

async function copyText(value) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }
  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand("copy");
  textarea.remove();
}

function makeCopyButton(value, label = "複製") {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "copy-button";
  button.setAttribute("aria-label", `${label}：${value.slice(0, 80)}`);
  button.innerHTML = '<span aria-hidden="true">⧉</span><span data-copy-label>複製</span>';
  button.addEventListener("click", async () => {
    await copyText(value);
    const output = button.querySelector("[data-copy-label]");
    output.textContent = "已複製";
    button.classList.add("copied");
    window.setTimeout(() => {
      output.textContent = "複製";
      button.classList.remove("copied");
    }, 1400);
  });
  return button;
}

function looksCopyable(value) {
  const text = value.trim();
  return (
    text === "run_id"
    || text.includes("/")
    || text.includes("\\")
    || text.startsWith("http://")
    || text.startsWith("https://")
    || /\.(json|md|html|py|rs|yaml|yml|csv|parquet)$/i.test(text)
  );
}

function enhanceCopyTargets() {
  document.querySelectorAll("pre:not(.mermaid)").forEach((block) => {
    if (block.querySelector(":scope > .copy-button")) return;
    const value = block.textContent.trim();
    if (value) block.appendChild(makeCopyButton(value, "複製程式碼"));
  });

  document.querySelectorAll("code:not(pre code)").forEach((code) => {
    const value = code.textContent.trim();
    if (!looksCopyable(value) || code.parentElement?.classList.contains("copy-inline")) return;
    const wrapper = document.createElement("span");
    wrapper.className = "copy-inline";
    code.replaceWith(wrapper);
    wrapper.append(code, makeCopyButton(value, "複製路徑或識別碼"));
  });
}

function readStoredChecklist(storageKey) {
  try {
    const value = JSON.parse(localStorage.getItem(storageKey) || "[]");
    return Array.isArray(value) ? value : [];
  } catch {
    localStorage.removeItem(storageKey);
    return [];
  }
}

function enhanceChecklists() {
  document.querySelectorAll("[data-checklist]").forEach((list) => {
    const storageKey = `lo2cin4bt-lecture-checklist:${list.dataset.checklist}`;
    const saved = readStoredChecklist(storageKey);
    const items = [...list.querySelectorAll(":scope > li")];
    items.forEach((item, index) => {
      const label = document.createElement("label");
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.checked = Boolean(saved[index]);
      while (item.firstChild) label.appendChild(item.firstChild);
      label.prepend(checkbox);
      item.appendChild(label);
      checkbox.addEventListener("change", () => {
        localStorage.setItem(
          storageKey,
          JSON.stringify(items.map((row) => row.querySelector("input")?.checked || false)),
        );
      });
    });

    const toolbar = document.createElement("div");
    toolbar.className = "checklist-toolbar";
    const reset = document.createElement("button");
    reset.type = "button";
    reset.textContent = "重設 Checklist";
    reset.addEventListener("click", () => {
      items.forEach((item) => { item.querySelector("input").checked = false; });
      localStorage.removeItem(storageKey);
    });
    const copy = makeCopyButton(
      items.map((item) => `- [ ] ${item.textContent.trim()}`).join("\n"),
      "複製 Checklist",
    );
    toolbar.append(copy, reset);
    list.insertAdjacentElement("beforebegin", toolbar);
  });
}

function buildToc() {
  const article = document.querySelector("article");
  if (!article || article.querySelector(".toc")) return;
  const headings = [...article.querySelectorAll("h2")];
  if (headings.length < 3) return;

  const toc = document.createElement("details");
  toc.className = "toc";
  toc.innerHTML = `<summary>本頁內容</summary><nav>${headings.map((heading, index) => {
    if (!heading.id) heading.id = `section-${index + 1}`;
    return `<a href="#${heading.id}">${heading.textContent}</a>`;
  }).join("")}</nav>`;
  const firstParagraph = article.querySelector("h1 + p, .lesson-meta + h1 + p");
  if (firstParagraph) firstParagraph.insertAdjacentElement("afterend", toc);
}

function enhanceReferenceTables() {
  if (currentLecturePath() !== "Module_07_Backtests_Report/index.html") return;
  document.querySelectorAll(".table-scroll").forEach((tableWrapper, index) => {
    const details = document.createElement("details");
    details.className = "reference-disclosure";
    if (index === 0) details.open = true;
    const summary = document.createElement("summary");
    summary.textContent = index === 0
      ? "核心績效與風險指標"
      : index === 1
        ? "交易品質與成本指標"
        : "Portfolio、Rebalance 與資料健康指標";
    tableWrapper.replaceWith(details);
    details.append(summary, tableWrapper);
  });
}

function addBackToTop() {
  const button = document.createElement("button");
  button.className = "back-to-top";
  button.type = "button";
  button.textContent = "頂部";
  button.title = "回到頁頂";
  button.addEventListener("click", () => scrollTo({ top: 0, behavior: "smooth" }));
  document.body.appendChild(button);
}

function loadMermaid() {
  if (!document.querySelector(".mermaid")) return Promise.resolve(false);
  if (window.mermaid) return Promise.resolve(true);

  return new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js";
    script.onload = () => resolve(true);
    script.onerror = reject;
    document.head.appendChild(script);
  });
}

async function renderMermaid() {
  try {
    const hasMermaid = await loadMermaid();
    if (!hasMermaid || !window.mermaid) return;
    window.mermaid.initialize({
      startOnLoad: false,
      securityLevel: "strict",
      theme: "base",
      themeVariables: {
        background: "#09131d",
        primaryColor: "#122434",
        primaryTextColor: "#f3efe4",
        primaryBorderColor: "#5bc7c4",
        lineColor: "#62bfc2",
        secondaryColor: "#1b2f3d",
        tertiaryColor: "#0c1924",
        edgeLabelBackground: "#0c1924",
        clusterBkg: "#0c1924",
        clusterBorder: "#d5ad63",
        fontFamily: '"Noto Sans TC", "Microsoft JhengHei", sans-serif',
      },
      flowchart: { htmlLabels: true, curve: "basis", useMaxWidth: true },
    });
    await window.mermaid.run({ querySelector: ".mermaid" });
    enhanceMermaidDiagrams();
  } catch (error) {
    document.querySelectorAll(".mermaid").forEach((block) => {
      block.dataset.fallback = "Mermaid 暫時無法載入；圖內 source 仍保留完整流程。";
      block.classList.add("mermaid-failed");
    });
    console.error("Mermaid failed to render", error);
  }
}

function createDiagramViewer() {
  const dialog = document.createElement("dialog");
  dialog.className = "diagram-viewer";
  dialog.setAttribute("aria-label", "Mermaid 圖表放大檢視");
  dialog.innerHTML = `
    <div class="diagram-viewer-shell">
      <header class="diagram-viewer-toolbar">
        <div>
          <span class="diagram-viewer-kicker">Diagram viewer</span>
          <strong data-diagram-title>流程圖</strong>
        </div>
        <div class="diagram-viewer-actions">
          <button type="button" data-zoom-out aria-label="縮小圖表">−</button>
          <output data-zoom-label>100%</output>
          <button type="button" data-zoom-in aria-label="放大圖表">＋</button>
          <button type="button" data-zoom-reset>重設</button>
          <button type="button" data-open-window>新視窗</button>
          <button type="button" class="diagram-viewer-close" data-close-viewer aria-label="關閉圖表">關閉</button>
        </div>
      </header>
      <div class="diagram-viewer-stage" data-diagram-stage></div>
    </div>
  `;

  const stage = dialog.querySelector("[data-diagram-stage]");
  const label = dialog.querySelector("[data-zoom-label]");
  let scale = 1;

  const updateScale = () => {
    const svg = stage.querySelector("svg");
    if (!svg) return;
    svg.style.width = `${scale * 100}%`;
    svg.style.maxWidth = "none";
    label.value = `${Math.round(scale * 100)}%`;
    label.textContent = label.value;
  };

  const setScale = (nextScale) => {
    scale = Math.min(4, Math.max(0.5, nextScale));
    updateScale();
  };

  dialog.openDiagram = (svg, title) => {
    scale = 1;
    stage.replaceChildren(svg.cloneNode(true));
    dialog.querySelector("[data-diagram-title]").textContent = title;
    updateScale();
    if (typeof dialog.showModal === "function") dialog.showModal();
    else dialog.setAttribute("open", "");
  };

  dialog.querySelector("[data-zoom-out]").addEventListener("click", () => setScale(scale - 0.25));
  dialog.querySelector("[data-zoom-in]").addEventListener("click", () => setScale(scale + 0.25));
  dialog.querySelector("[data-zoom-reset]").addEventListener("click", () => setScale(1));
  dialog.querySelector("[data-close-viewer]").addEventListener("click", () => dialog.close());
  dialog.addEventListener("click", (event) => {
    if (event.target === dialog) dialog.close();
  });

  dialog.querySelector("[data-open-window]").addEventListener("click", () => {
    const svg = stage.querySelector("svg");
    if (!svg) return;
    const title = dialog.querySelector("[data-diagram-title]").textContent;
    const documentSource = `<!doctype html>
      <html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
      <title>${title}</title><style>
      html,body{margin:0;min-height:100%;background:#071019;color:#f3efe4;font-family:"Microsoft JhengHei",sans-serif}
      header{position:sticky;top:0;padding:14px 20px;border-bottom:1px solid #253847;background:rgba(7,16,25,.94);z-index:2}
      main{min-width:max-content;padding:28px}svg{display:block;width:auto;min-width:calc(100vw - 56px);height:auto;max-width:none}
      </style></head><body><header>${title} · 使用瀏覽器縮放或另存圖像</header><main>${svg.outerHTML}</main></body></html>`;
    const url = URL.createObjectURL(new Blob([documentSource], { type: "text/html" }));
    window.open(url, "_blank", "noopener,noreferrer");
    window.setTimeout(() => URL.revokeObjectURL(url), 60_000);
  });

  document.body.appendChild(dialog);
  return dialog;
}

function enhanceMermaidDiagrams() {
  const diagrams = [...document.querySelectorAll(".mermaid")];
  if (!diagrams.length) return;
  const viewer = document.querySelector(".diagram-viewer") || createDiagramViewer();

  diagrams.forEach((diagram, index) => {
    if (diagram.dataset.viewerReady === "true") return;
    const svg = diagram.querySelector("svg");
    if (!svg) return;
    const panel = diagram.closest(".mermaid-panel");
    const title = panel?.querySelector("h2")?.textContent?.trim() || `流程圖 ${index + 1}`;
    diagram.dataset.viewerReady = "true";
    diagram.tabIndex = 0;
    diagram.setAttribute("role", "button");
    diagram.setAttribute("aria-label", `放大檢視：${title}`);

    const hint = document.createElement("span");
    hint.className = "diagram-expand-hint";
    hint.textContent = "點擊放大";
    diagram.appendChild(hint);

    const open = () => viewer.openDiagram(svg, title);
    diagram.addEventListener("click", open);
    diagram.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        open();
      }
    });
  });
}

document.addEventListener("DOMContentLoaded", () => {
  buildSidebar();
  buildMobileNavigation();
  enableSearch();
  buildLessonMeta();
  buildCourseStepper();
  buildProgress();
  buildToc();
  enhanceTermHelp();
  enhanceChecklists();
  enhanceCopyTargets();
  enhanceReferenceTables();
  renderMermaid();
  addBackToTop();
});
