from __future__ import annotations

import queue
import sys
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

from dashboard_support import (
    ASSETS_DIR,
    BASE_DIR,
    ICON_ICO_PATH,
    ICON_PNG_PATH,
    INPUT_FOLDERS,
    LATEST_DIR,
    RUNS_DIR,
    WORKFLOWS,
    copy_files_to_input,
    ensure_runtime_dirs,
    list_input_files,
    list_latest_outputs,
    list_recent_runs,
    open_path,
    run_workflow,
)


PALETTE = {
    "bg": "#f4eee5",
    "panel": "#fffaf4",
    "card": "#ffffff",
    "ink": "#22313f",
    "muted": "#5f6b76",
    "accent": "#c16e33",
    "accent_dark": "#9f5422",
    "accent_soft": "#ead5c2",
    "border": "#d9c8b3",
    "success": "#2f6b4f",
    "info": "#dde8ee",
}


INPUT_EXAMPLES = {
    "price_lists": "Examples: vendor CSV, PDF, or Excel price sheets.",
    "square_exports": "Examples: same-night Square item-library export before receiving.",
    "deliveries": "Examples: packing slip, PO detail, or vendor shipment sheet.",
    "adjustments": "Examples: recount fixes, breakage, or opening balance corrections.",
    "sales": "Examples: Square Sales by item export for the date range you want to analyze.",
    "price_updates": "Examples: manual selling-price changes you want preserved.",
    "pricing_overrides": "Examples: SKUs that need a forced price or custom target margin.",
    "sales_match_overrides": "Examples: store-created shorthand names mapped to a catalog SKU.",
}


WORKFLOW_PLAYBOOKS = {
    "master_inventory": {
        "best_for": "Use when vendor price lists changed or you want to refresh the core catalog.",
        "steps": (
            "Upload the newest vendor price lists.",
            "Run the master inventory builder.",
            "Review the overlap and enrichment audit files.",
        ),
        "outputs": (
            "Master Square import file",
            "Overlap review file",
            "Image and product enrichment audits",
        ),
    },
    "sales_match": {
        "best_for": "Use when you want sales-aware pricing or need to connect store sales names back to the catalog.",
        "steps": (
            "Upload a Square Sales by item export.",
            "Add any manual sales-match overrides if needed.",
            "Run the sales matching workflow and review weak matches.",
        ),
        "outputs": (
            "Sales-to-catalog match audit",
            "Sales review queue",
            "Sales catalog signals used by pricing",
        ),
    },
    "pricing": {
        "best_for": "Use when you want recommended selling prices or a Square-ready price update file.",
        "steps": (
            "Optionally run sales matching first for sales-aware pricing.",
            "Add any manual pricing overrides.",
            "Run pricing and review the recommendation workbook.",
        ),
        "outputs": (
            "Pricing recommendations workbook",
            "Strategic Square import file",
            "Square-ready catalog price update file",
        ),
    },
    "receiving": {
        "best_for": "Use after hours when you exported Square, received stock, and want to import only the changed quantity rows.",
        "steps": (
            "Upload the fresh Square export from tonight.",
            "Upload the delivery or packing-slip file.",
            "Run the receiving workflow and import the update back into Square the same night.",
        ),
        "outputs": (
            "Small Square receiving update file",
            "Receiving audit",
            "Issues file for unmatched rows",
        ),
    },
    "stock_snapshot": {
        "best_for": "Use when you want internal stock and price snapshots across deliveries, adjustments, and price updates.",
        "steps": (
            "Upload delivery, adjustment, and price update files.",
            "Run the stock snapshot workflow.",
            "Open the snapshot and Square update files.",
        ),
        "outputs": (
            "Current stock snapshot",
            "Current pricing snapshot",
            "Square quantity and price update files",
        ),
    },
}


HOME_RECIPES = (
    {
        "title": "Tonight's Delivery",
        "subtitle": "Best for same-night stock receiving.",
        "steps": (
            "Upload a fresh Square export to Square Exports.",
            "Upload the packing slip to Deliveries.",
            "Run Build After-Hours Receiving Import.",
        ),
        "tab": "Workflows",
    },
    {
        "title": "Catalog Refresh",
        "subtitle": "Best for new vendor pricing or catalog cleanup.",
        "steps": (
            "Upload the newest vendor price lists.",
            "Run Build Master Inventory.",
            "Open the overlap review and latest master import.",
        ),
        "tab": "Workflows",
    },
    {
        "title": "Pricing Review",
        "subtitle": "Best for strategic pricing changes.",
        "steps": (
            "Upload a Sales by item report if you want sales-aware pricing.",
            "Run Match Sales History, then Build Pricing Recommendations.",
            "Open the latest pricing workbook and price import file.",
        ),
        "tab": "Workflows",
    },
)


class InventoryDashboard(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        ensure_runtime_dirs()
        self.title("AZCS Inventory Control Center")
        self.geometry("1380x900")
        self.minsize(1180, 780)
        self.configure(bg=PALETTE["bg"])

        self.event_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self.busy_workflow_key: str | None = None

        self.input_widgets: dict[str, dict[str, object]] = {}
        self.workflow_widgets: dict[str, dict[str, object]] = {}
        self.latest_output_paths: list[Path] = []
        self.recent_run_paths: list[Path] = []
        self.workflow_order = list(WORKFLOWS.keys())
        self.selected_latest_workflow = tk.StringVar(value=self.workflow_order[0])

        self.metric_vars: dict[str, tk.StringVar] = {
            "input_files": tk.StringVar(value="0"),
            "latest_ready": tk.StringVar(value="0"),
            "archived_runs": tk.StringVar(value="0"),
            "workflows": tk.StringVar(value=str(len(WORKFLOWS))),
        }
        self.outputs_summary_var = tk.StringVar(value="Choose a workflow to see its latest files.")
        self.outputs_subtitle_var = tk.StringVar(value="")

        self.brand_icon: tk.PhotoImage | None = None
        self.latest_log_text: ScrolledText | None = None

        self._apply_icon()
        self._configure_style()
        self._build_layout()
        self.refresh_all()
        self.after(250, self._poll_events)

    def _apply_icon(self) -> None:
        if ICON_ICO_PATH.exists():
            try:
                self.iconbitmap(default=str(ICON_ICO_PATH))
            except tk.TclError:
                pass
        if ICON_PNG_PATH.exists():
            try:
                self.brand_icon = tk.PhotoImage(file=str(ICON_PNG_PATH))
                self.iconphoto(True, self.brand_icon)
            except tk.TclError:
                self.brand_icon = None

    def _configure_style(self) -> None:
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure(".", font=("Segoe UI", 10))
        style.configure("App.TNotebook", background=PALETTE["bg"], borderwidth=0)
        style.configure(
            "App.TNotebook.Tab",
            padding=(18, 10),
            background=PALETTE["accent_soft"],
            foreground=PALETTE["ink"],
            font=("Segoe UI", 10, "bold"),
        )
        style.map(
            "App.TNotebook.Tab",
            background=[("selected", PALETTE["card"]), ("active", "#f1dfcf")],
            foreground=[("selected", PALETTE["accent_dark"])],
        )

        style.configure("Primary.TButton", font=("Segoe UI", 10, "bold"), padding=(12, 8))
        style.configure("Secondary.TButton", padding=(10, 7))

    def _build_layout(self) -> None:
        container = tk.Frame(self, bg=PALETTE["bg"])
        container.pack(fill="both", expand=True, padx=14, pady=14)

        self._build_header(container)

        self.notebook = ttk.Notebook(container, style="App.TNotebook")
        self.notebook.pack(fill="both", expand=True, pady=(12, 0))

        self.tabs: dict[str, ttk.Frame] = {}
        for name in ("Home", "Uploads", "Workflows", "Outputs"):
            frame = ttk.Frame(self.notebook, padding=12)
            self.tabs[name] = frame
            self.notebook.add(frame, text=name)

        self.home_tab = self.tabs["Home"]
        self.uploads_tab = self.tabs["Uploads"]
        self.workflows_tab = self.tabs["Workflows"]
        self.outputs_tab = self.tabs["Outputs"]

        self._build_home_tab()
        self._build_uploads_tab()
        self._build_workflows_tab()
        self._build_outputs_tab()
        self._build_log(container)

    def _build_header(self, parent: tk.Widget) -> None:
        hero = tk.Frame(parent, bg=PALETTE["ink"], highlightthickness=1, highlightbackground=PALETTE["border"])
        hero.pack(fill="x")

        left = tk.Frame(hero, bg=PALETTE["ink"])
        left.pack(side="left", fill="x", expand=True, padx=16, pady=16)

        if self.brand_icon is not None:
            scaled = self.brand_icon.subsample(4, 4)
            self.header_icon = scaled
            tk.Label(left, image=scaled, bg=PALETTE["ink"]).pack(side="left", padx=(0, 14))

        text_wrap = tk.Frame(left, bg=PALETTE["ink"])
        text_wrap.pack(side="left", fill="x", expand=True)
        tk.Label(
            text_wrap,
            text="AZCS Inventory Control Center",
            font=("Segoe UI", 22, "bold"),
            fg="#ffffff",
            bg=PALETTE["ink"],
        ).pack(anchor="w")
        tk.Label(
            text_wrap,
            text="A cleaner front door for uploads, workflows, latest outputs, and run history.",
            font=("Segoe UI", 10),
            fg="#d8e1e8",
            bg=PALETTE["ink"],
        ).pack(anchor="w", pady=(4, 0))

        actions = tk.Frame(hero, bg=PALETTE["ink"])
        actions.pack(side="right", padx=16, pady=16)
        ttk.Button(actions, text="Open Repo Folder", style="Secondary.TButton", command=lambda: self._open_path(BASE_DIR)).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Open Latest Folder", style="Secondary.TButton", command=lambda: self._open_path(LATEST_DIR)).pack(side="left")

    def _build_log(self, parent: tk.Widget) -> None:
        log_frame = ttk.LabelFrame(parent, text="Run Log", padding=10)
        log_frame.pack(fill="both", expand=False, pady=(12, 0))
        self.log_text = ScrolledText(
            log_frame,
            height=8,
            wrap="word",
            font=("Consolas", 10),
            bg=PALETTE["card"],
            fg=PALETTE["ink"],
            relief="flat",
        )
        self.log_text.pack(fill="both", expand=True)
        self.log_text.insert("end", f"[{self._now()}] Dashboard ready.\n")
        self.log_text.configure(state="disabled")

    def _card(self, parent: tk.Widget, title: str, subtitle: str) -> tk.Frame:
        frame = tk.Frame(parent, bg=PALETTE["panel"], highlightthickness=1, highlightbackground=PALETTE["border"])
        tk.Label(frame, text=title, font=("Segoe UI", 15, "bold"), bg=PALETTE["panel"], fg=PALETTE["ink"]).pack(anchor="w", padx=14, pady=(12, 0))
        tk.Label(frame, text=subtitle, font=("Segoe UI", 10), bg=PALETTE["panel"], fg=PALETTE["muted"], wraplength=1080, justify="left").pack(anchor="w", padx=14, pady=(4, 12))
        return frame

    def _build_home_tab(self) -> None:
        metrics_frame = tk.Frame(self.home_tab, bg=PALETTE["bg"])
        metrics_frame.pack(fill="x")

        metrics = [
            ("Input Files", self.metric_vars["input_files"], PALETTE["accent_soft"]),
            ("Latest Folders Ready", self.metric_vars["latest_ready"], PALETTE["info"]),
            ("Archived Runs", self.metric_vars["archived_runs"], "#e7f0e4"),
            ("Workflows", self.metric_vars["workflows"], "#efe6fb"),
        ]
        for index, (label, variable, bg_color) in enumerate(metrics):
            card = tk.Frame(metrics_frame, bg=bg_color, highlightthickness=1, highlightbackground=PALETTE["border"])
            card.grid(row=0, column=index, sticky="nsew", padx=(0, 10 if index < len(metrics) - 1 else 0))
            metrics_frame.grid_columnconfigure(index, weight=1)
            value_label = tk.Label(card, text=variable.get(), font=("Segoe UI", 22, "bold"), bg=bg_color, fg=PALETTE["ink"])
            value_label.pack(anchor="w", padx=14, pady=(12, 0))
            tk.Label(card, text=label, font=("Segoe UI", 10), bg=bg_color, fg=PALETTE["muted"]).pack(anchor="w", padx=14, pady=(2, 12))
            variable.trace_add("write", lambda *_args, var=variable, target=value_label: target.configure(text=var.get()))

        body = tk.Frame(self.home_tab, bg=PALETTE["bg"])
        body.pack(fill="both", expand=True, pady=(14, 0))
        body.grid_columnconfigure(0, weight=3)
        body.grid_columnconfigure(1, weight=2)

        recipes_card = self._card(body, "Most Common Jobs", "Start here if you are not sure which workflow to run.")
        recipes_card.grid(row=0, column=0, sticky="nsew", padx=(0, 10))

        for recipe in HOME_RECIPES:
            recipe_frame = tk.Frame(recipes_card, bg=PALETTE["card"], highlightthickness=1, highlightbackground=PALETTE["border"])
            recipe_frame.pack(fill="x", pady=(0, 10))
            tk.Label(recipe_frame, text=recipe["title"], font=("Segoe UI", 12, "bold"), bg=PALETTE["card"], fg=PALETTE["ink"]).pack(anchor="w", padx=12, pady=(10, 2))
            tk.Label(recipe_frame, text=recipe["subtitle"], font=("Segoe UI", 10), bg=PALETTE["card"], fg=PALETTE["muted"]).pack(anchor="w", padx=12)
            for idx, step in enumerate(recipe["steps"], start=1):
                tk.Label(
                    recipe_frame,
                    text=f"{idx}. {step}",
                    font=("Segoe UI", 10),
                    bg=PALETTE["card"],
                    fg=PALETTE["ink"],
                    wraplength=620,
                    justify="left",
                ).pack(anchor="w", padx=12, pady=(6 if idx == 1 else 3, 0))
            ttk.Button(recipe_frame, text=f"Go to {recipe['tab']}", command=lambda tab=recipe["tab"]: self._select_tab(tab)).pack(anchor="w", padx=12, pady=12)

        guide_card = self._card(body, "Plain-English Guide", "These rules make the dashboard easier for someone new to use.")
        guide_card.grid(row=0, column=1, sticky="nsew")
        guide_lines = [
            "Uploads is where new files go. Nothing runs from there until you click a workflow button.",
            "Workflows is where processing happens. Run one workflow at a time.",
            "Outputs shows the newest successful files in clean folders, so you do not have to sort through the whole repo.",
            "Runs keeps a timestamped archive of each successful workflow run and its log.",
            "For receiving, always use a fresh Square export from the same after-hours session.",
        ]
        for line in guide_lines:
            tk.Label(guide_card, text=f"- {line}", font=("Segoe UI", 10), bg=PALETTE["panel"], fg=PALETTE["ink"], wraplength=420, justify="left").pack(anchor="w", pady=4)

        quick_card = tk.Frame(guide_card, bg=PALETTE["panel"])
        quick_card.pack(fill="x", pady=(14, 0))
        ttk.Button(quick_card, text="Open Uploads", command=lambda: self._select_tab("Uploads")).pack(side="left", padx=(0, 8))
        ttk.Button(quick_card, text="Open Workflows", command=lambda: self._select_tab("Workflows")).pack(side="left", padx=(0, 8))
        ttk.Button(quick_card, text="Open Outputs", command=lambda: self._select_tab("Outputs")).pack(side="left")

    def _build_uploads_tab(self) -> None:
        intro = self._card(
            self.uploads_tab,
            "Uploads",
            "Copy new files into the right folders here. The dashboard does the filing so the scripts can stay simple.",
        )
        intro.pack(fill="both", expand=True)

        grid = tk.Frame(intro, bg=PALETTE["panel"])
        grid.pack(fill="both", expand=True, pady=(8, 0))
        grid.grid_columnconfigure(0, weight=1)
        grid.grid_columnconfigure(1, weight=1)

        for index, folder in enumerate(INPUT_FOLDERS.values()):
            card = tk.Frame(grid, bg=PALETTE["card"], highlightthickness=1, highlightbackground=PALETTE["border"])
            row = index // 2
            column = index % 2
            card.grid(row=row, column=column, sticky="nsew", padx=(0, 10) if column == 0 else (0, 0), pady=(0, 10))

            tk.Label(card, text=folder.label, font=("Segoe UI", 12, "bold"), bg=PALETTE["card"], fg=PALETTE["ink"]).pack(anchor="w", padx=12, pady=(10, 2))
            tk.Label(card, text=folder.description, font=("Segoe UI", 10), bg=PALETTE["card"], fg=PALETTE["muted"], wraplength=520, justify="left").pack(anchor="w", padx=12)
            tk.Label(card, text=INPUT_EXAMPLES.get(folder.key, ""), font=("Segoe UI", 9), bg=PALETTE["card"], fg=PALETTE["muted"], wraplength=520, justify="left").pack(anchor="w", padx=12, pady=(6, 0))

            meta = tk.Frame(card, bg=PALETTE["card"])
            meta.pack(fill="x", padx=12, pady=(10, 8))
            count_var = tk.StringVar(value="0 files")
            count_badge = tk.Label(
                meta,
                textvariable=count_var,
                font=("Segoe UI", 9, "bold"),
                bg=PALETTE["accent_soft"],
                fg=PALETTE["accent_dark"],
                padx=8,
                pady=3,
            )
            count_badge.pack(side="left")
            tk.Label(meta, text=str(folder.path), font=("Segoe UI", 9), bg=PALETTE["card"], fg=PALETTE["muted"]).pack(side="right")

            listbox = tk.Listbox(
                card,
                height=5,
                bg="#fbfaf7",
                fg=PALETTE["ink"],
                relief="flat",
                highlightthickness=1,
                highlightbackground=PALETTE["border"],
                selectbackground=PALETTE["accent"],
                selectforeground="#ffffff",
            )
            listbox.pack(fill="x", padx=12)
            listbox.bind("<Double-Button-1>", lambda event, key=folder.key: self._open_selected_input_file(key))

            buttons = tk.Frame(card, bg=PALETTE["card"])
            buttons.pack(fill="x", padx=12, pady=12)
            ttk.Button(buttons, text="Add Files", style="Primary.TButton", command=lambda key=folder.key: self._add_files(key)).pack(side="left", padx=(0, 8))
            ttk.Button(buttons, text="Open Folder", command=lambda path=folder.path: self._open_path(path)).pack(side="left", padx=(0, 8))
            ttk.Button(buttons, text="Open Selected File", command=lambda key=folder.key: self._open_selected_input_file(key)).pack(side="left")

            self.input_widgets[folder.key] = {
                "count_var": count_var,
                "count_badge": count_badge,
                "listbox": listbox,
                "paths": [],
            }

    def _build_workflows_tab(self) -> None:
        shell = self._card(
            self.workflows_tab,
            "Workflows",
            "Each card explains when to use the workflow, what to upload first, and which files you will get back.",
        )
        shell.pack(fill="both", expand=True)

        canvas = tk.Canvas(shell, bg=PALETTE["panel"], highlightthickness=0)
        scrollbar = ttk.Scrollbar(shell, orient="vertical", command=canvas.yview)
        scroller = tk.Frame(canvas, bg=PALETTE["panel"])
        scroller.bind("<Configure>", lambda event: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scroller, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        for workflow in WORKFLOWS.values():
            playbook = WORKFLOW_PLAYBOOKS.get(workflow.key, {"best_for": "", "steps": (), "outputs": ()})
            card = tk.Frame(scroller, bg=PALETTE["card"], highlightthickness=1, highlightbackground=PALETTE["border"])
            card.pack(fill="x", pady=(0, 10))

            accent_bar = tk.Frame(card, bg=PALETTE["accent"], width=8)
            accent_bar.pack(side="left", fill="y")

            body = tk.Frame(card, bg=PALETTE["card"])
            body.pack(side="left", fill="both", expand=True, padx=14, pady=12)

            header = tk.Frame(body, bg=PALETTE["card"])
            header.pack(fill="x")
            tk.Label(header, text=workflow.name, font=("Segoe UI", 13, "bold"), bg=PALETTE["card"], fg=PALETTE["ink"]).pack(anchor="w")
            tk.Label(header, text=playbook["best_for"], font=("Segoe UI", 10), bg=PALETTE["card"], fg=PALETTE["muted"], wraplength=980, justify="left").pack(anchor="w", pady=(3, 0))

            chips = tk.Frame(body, bg=PALETTE["card"])
            chips.pack(fill="x", pady=(10, 0))
            for key in workflow.input_keys:
                tk.Label(
                    chips,
                    text=INPUT_FOLDERS[key].label,
                    font=("Segoe UI", 9, "bold"),
                    bg=PALETTE["accent_soft"],
                    fg=PALETTE["accent_dark"],
                    padx=8,
                    pady=4,
                ).pack(side="left", padx=(0, 6))

            details = tk.Frame(body, bg=PALETTE["card"])
            details.pack(fill="x", pady=(10, 0))
            left = tk.Frame(details, bg=PALETTE["card"])
            right = tk.Frame(details, bg=PALETTE["card"])
            left.pack(side="left", fill="both", expand=True)
            right.pack(side="left", fill="both", expand=True, padx=(20, 0))

            tk.Label(left, text="What to do", font=("Segoe UI", 10, "bold"), bg=PALETTE["card"], fg=PALETTE["ink"]).pack(anchor="w")
            for index, step in enumerate(playbook["steps"], start=1):
                tk.Label(left, text=f"{index}. {step}", font=("Segoe UI", 10), bg=PALETTE["card"], fg=PALETTE["ink"], wraplength=460, justify="left").pack(anchor="w", pady=3)

            tk.Label(right, text="What you get", font=("Segoe UI", 10, "bold"), bg=PALETTE["card"], fg=PALETTE["ink"]).pack(anchor="w")
            for output in playbook["outputs"]:
                tk.Label(right, text=f"- {output}", font=("Segoe UI", 10), bg=PALETTE["card"], fg=PALETTE["ink"], wraplength=420, justify="left").pack(anchor="w", pady=3)

            footer = tk.Frame(body, bg=PALETTE["card"])
            footer.pack(fill="x", pady=(12, 0))
            status_var = tk.StringVar(value="Ready.")
            run_button = ttk.Button(
                footer,
                text="Run Workflow",
                style="Primary.TButton",
                command=lambda key=workflow.key: self._run_workflow_async(key),
            )
            run_button.pack(side="left", padx=(0, 8))
            ttk.Button(footer, text="Open Latest Folder", command=lambda key=workflow.key: self._open_path(LATEST_DIR / key)).pack(side="left", padx=(0, 8))
            ttk.Button(footer, text="Go to Outputs", command=lambda key=workflow.key: self._go_to_outputs_for_workflow(key)).pack(side="left")
            tk.Label(footer, textvariable=status_var, font=("Segoe UI", 10, "bold"), bg=PALETTE["card"], fg=PALETTE["muted"]).pack(side="right")

            self.workflow_widgets[workflow.key] = {
                "status_var": status_var,
                "run_button": run_button,
            }

    def _build_outputs_tab(self) -> None:
        shell = self._card(
            self.outputs_tab,
            "Outputs",
            "Open the newest successful files without digging through the full repo. Each workflow also keeps timestamped run archives.",
        )
        shell.pack(fill="both", expand=True)

        controls = tk.Frame(shell, bg=PALETTE["panel"])
        controls.pack(fill="x")
        tk.Label(controls, text="Workflow", font=("Segoe UI", 10, "bold"), bg=PALETTE["panel"], fg=PALETTE["ink"]).pack(side="left")
        self.latest_selector = ttk.Combobox(
            controls,
            values=[WORKFLOWS[key].name for key in self.workflow_order],
            state="readonly",
            width=34,
        )
        self.latest_selector.pack(side="left", padx=(8, 8))
        self.latest_selector.current(0)
        self.latest_selector.bind("<<ComboboxSelected>>", lambda event: self._on_latest_workflow_change(self.latest_selector.current()))
        ttk.Button(controls, text="Refresh", command=self.refresh_outputs).pack(side="left", padx=(0, 8))
        ttk.Button(controls, text="Open Latest Folder", command=self._open_current_latest_folder).pack(side="left", padx=(0, 8))
        ttk.Button(controls, text="Open Runs Folder", command=lambda: self._open_path(RUNS_DIR)).pack(side="left")

        summary = tk.Frame(shell, bg=PALETTE["card"], highlightthickness=1, highlightbackground=PALETTE["border"])
        summary.pack(fill="x", pady=(12, 0))
        tk.Label(summary, textvariable=self.outputs_summary_var, font=("Segoe UI", 11, "bold"), bg=PALETTE["card"], fg=PALETTE["ink"]).pack(anchor="w", padx=12, pady=(10, 2))
        tk.Label(summary, textvariable=self.outputs_subtitle_var, font=("Segoe UI", 10), bg=PALETTE["card"], fg=PALETTE["muted"], wraplength=1080, justify="left").pack(anchor="w", padx=12, pady=(0, 10))

        panes = ttk.Panedwindow(shell, orient="horizontal")
        panes.pack(fill="both", expand=True, pady=(12, 0))

        latest_frame = ttk.LabelFrame(panes, text="Latest Files", padding=10)
        runs_frame = ttk.LabelFrame(panes, text="Recent Runs", padding=10)
        log_frame = ttk.LabelFrame(panes, text="Run Summary", padding=10)
        panes.add(latest_frame, weight=2)
        panes.add(runs_frame, weight=1)
        panes.add(log_frame, weight=2)

        self.latest_listbox = tk.Listbox(
            latest_frame,
            bg="#fbfaf7",
            fg=PALETTE["ink"],
            relief="flat",
            highlightthickness=1,
            highlightbackground=PALETTE["border"],
            selectbackground=PALETTE["accent"],
            selectforeground="#ffffff",
        )
        self.latest_listbox.pack(fill="both", expand=True)
        self.latest_listbox.bind("<Double-Button-1>", lambda event: self._open_selected_latest_file())

        latest_buttons = tk.Frame(latest_frame, bg=PALETTE["panel"])
        latest_buttons.pack(fill="x", pady=(8, 0))
        ttk.Button(latest_buttons, text="Open Selected File", command=self._open_selected_latest_file).pack(side="left")

        self.recent_runs_listbox = tk.Listbox(
            runs_frame,
            bg="#fbfaf7",
            fg=PALETTE["ink"],
            relief="flat",
            highlightthickness=1,
            highlightbackground=PALETTE["border"],
            selectbackground=PALETTE["accent"],
            selectforeground="#ffffff",
        )
        self.recent_runs_listbox.pack(fill="both", expand=True)
        self.recent_runs_listbox.bind("<<ListboxSelect>>", lambda event: self._preview_selected_run_log())
        self.recent_runs_listbox.bind("<Double-Button-1>", lambda event: self._open_selected_run_folder())

        runs_buttons = tk.Frame(runs_frame, bg=PALETTE["panel"])
        runs_buttons.pack(fill="x", pady=(8, 0))
        ttk.Button(runs_buttons, text="Open Selected Run Folder", command=self._open_selected_run_folder).pack(side="left")

        self.latest_log_text = ScrolledText(
            log_frame,
            wrap="word",
            font=("Consolas", 10),
            bg=PALETTE["card"],
            fg=PALETTE["ink"],
            relief="flat",
        )
        self.latest_log_text.pack(fill="both", expand=True)
        self.latest_log_text.configure(state="disabled")

    def refresh_all(self) -> None:
        for key in INPUT_FOLDERS:
            self.refresh_input_folder(key)
        self.refresh_outputs()
        self._refresh_metrics()

    def refresh_input_folder(self, folder_key: str) -> None:
        files = list_input_files(folder_key)
        widgets = self.input_widgets[folder_key]
        listbox: tk.Listbox = widgets["listbox"]  # type: ignore[assignment]
        count_var: tk.StringVar = widgets["count_var"]  # type: ignore[assignment]
        count_badge: tk.Label = widgets["count_badge"]  # type: ignore[assignment]

        listbox.delete(0, "end")
        if files:
            for path in files:
                modified = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
                listbox.insert("end", f"{path.name}    ({modified})")
        else:
            listbox.insert("end", "No files here yet.")

        widgets["paths"] = files
        count_var.set(f"{len(files)} file{'s' if len(files) != 1 else ''}")
        count_badge.configure(
            bg=PALETTE["accent_soft"] if files else PALETTE["info"],
            fg=PALETTE["accent_dark"] if files else PALETTE["ink"],
        )
        self._refresh_metrics()

    def refresh_outputs(self) -> None:
        self._refresh_latest_files()
        self._refresh_recent_runs()
        self._refresh_outputs_summary()
        self._refresh_metrics()

    def _refresh_metrics(self) -> None:
        input_count = sum(len(list_input_files(key)) for key in INPUT_FOLDERS)
        latest_ready = 0
        for key in self.workflow_order:
            if list_latest_outputs(key):
                latest_ready += 1
        archived_runs = len([path for path in RUNS_DIR.iterdir() if path.is_dir()]) if RUNS_DIR.exists() else 0

        self.metric_vars["input_files"].set(str(input_count))
        self.metric_vars["latest_ready"].set(str(latest_ready))
        self.metric_vars["archived_runs"].set(str(archived_runs))

    def _refresh_latest_files(self) -> None:
        workflow_key = self.workflow_order[self.latest_selector.current()]
        self.selected_latest_workflow.set(workflow_key)
        self.latest_output_paths = list_latest_outputs(workflow_key)
        self.latest_listbox.delete(0, "end")
        if self.latest_output_paths:
            for path in self.latest_output_paths:
                modified = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
                self.latest_listbox.insert("end", f"{path.name}    ({modified})")
        else:
            self.latest_listbox.insert("end", "No latest files yet. Run this workflow first.")
        self._preview_latest_workflow_log()

    def _refresh_recent_runs(self) -> None:
        self.recent_run_paths = list_recent_runs(limit=20)
        self.recent_runs_listbox.delete(0, "end")
        if self.recent_run_paths:
            for path in self.recent_run_paths:
                self.recent_runs_listbox.insert("end", path.name)
        else:
            self.recent_runs_listbox.insert("end", "No archived runs yet.")

    def _refresh_outputs_summary(self) -> None:
        workflow_key = self.selected_latest_workflow.get()
        workflow = WORKFLOWS[workflow_key]
        playbook = WORKFLOW_PLAYBOOKS.get(workflow_key, {"best_for": "", "outputs": ()})
        latest_files = list_latest_outputs(workflow_key)
        self.outputs_summary_var.set(f"{workflow.name} | {len(latest_files)} latest file{'s' if len(latest_files) != 1 else ''} ready")
        outputs_text = ", ".join(playbook["outputs"]) if playbook["outputs"] else "Open the latest folder to review the files."
        self.outputs_subtitle_var.set(f"{playbook['best_for']} Main outputs: {outputs_text}.")

    def _on_latest_workflow_change(self, index: int) -> None:
        workflow_key = self.workflow_order[index]
        self.selected_latest_workflow.set(workflow_key)
        self._refresh_latest_files()
        self._refresh_outputs_summary()

    def _go_to_outputs_for_workflow(self, workflow_key: str) -> None:
        self._select_tab("Outputs")
        index = self.workflow_order.index(workflow_key)
        self.latest_selector.current(index)
        self._on_latest_workflow_change(index)

    def _select_tab(self, tab_name: str) -> None:
        self.notebook.select(self.tabs[tab_name])

    def _add_files(self, folder_key: str) -> None:
        selected = filedialog.askopenfilenames(
            title=f"Add files to {INPUT_FOLDERS[folder_key].label}",
            initialdir=str(Path.home() / "Downloads"),
        )
        if not selected:
            return
        copied = copy_files_to_input(folder_key, list(selected))
        self.refresh_input_folder(folder_key)
        self._log(f"Copied {len(copied)} file(s) into {INPUT_FOLDERS[folder_key].path}.")

    def _open_selected_input_file(self, folder_key: str) -> None:
        widgets = self.input_widgets[folder_key]
        listbox: tk.Listbox = widgets["listbox"]  # type: ignore[assignment]
        selection = listbox.curselection()
        paths: list[Path] = widgets["paths"]  # type: ignore[assignment]
        if not selection or not paths:
            messagebox.showinfo("Open File", "Select a real file first.")
            return
        self._open_path(paths[selection[0]])

    def _open_current_latest_folder(self) -> None:
        self._open_path(LATEST_DIR / self.selected_latest_workflow.get())

    def _open_selected_latest_file(self) -> None:
        selection = self.latest_listbox.curselection()
        if not selection or not self.latest_output_paths:
            messagebox.showinfo("Open File", "Select a latest output file first.")
            return
        self._open_path(self.latest_output_paths[selection[0]])

    def _open_selected_run_folder(self) -> None:
        selection = self.recent_runs_listbox.curselection()
        if not selection or not self.recent_run_paths:
            messagebox.showinfo("Open Run", "Select a run folder first.")
            return
        self._open_path(self.recent_run_paths[selection[0]])

    def _open_path(self, path: Path) -> None:
        if not path.exists():
            messagebox.showwarning("Missing Path", f"That path does not exist yet:\n{path}")
            return
        open_path(path)

    def _run_workflow_async(self, workflow_key: str) -> None:
        if self.busy_workflow_key is not None:
            messagebox.showinfo("Workflow Running", "Wait for the current workflow to finish before starting another one.")
            return

        self.busy_workflow_key = workflow_key
        workflow = WORKFLOWS[workflow_key]
        self._set_run_buttons_state("disabled")
        self.workflow_widgets[workflow_key]["status_var"].set("Running now...")
        self._log(f"Starting workflow: {workflow.name}")

        worker = threading.Thread(target=self._run_workflow_worker, args=(workflow_key,), daemon=True)
        worker.start()

    def _run_workflow_worker(self, workflow_key: str) -> None:
        try:
            result = run_workflow(workflow_key)
            self.event_queue.put(("run_result", result))
        except Exception as exc:  # pragma: no cover
            self.event_queue.put(("run_error", (workflow_key, str(exc))))

    def _poll_events(self) -> None:
        while True:
            try:
                event_type, payload = self.event_queue.get_nowait()
            except queue.Empty:
                break

            if event_type == "run_result":
                self._handle_run_result(payload)
            elif event_type == "run_error":
                workflow_key, message = payload  # type: ignore[misc]
                self._handle_run_error(workflow_key, message)

        self.after(250, self._poll_events)

    def _handle_run_result(self, result) -> None:
        workflow_key = result.workflow.key
        status_var = self.workflow_widgets[workflow_key]["status_var"]

        if result.success:
            status_var.set(f"Finished at {result.finished_at.strftime('%H:%M:%S')}")
            self._log(
                f"{result.workflow.name} finished successfully. "
                f"Copied {len(result.copied_outputs)} file(s) into {result.latest_dir}."
            )
            if result.missing_outputs:
                self._log(f"Missing expected outputs: {', '.join(result.missing_outputs)}")
        else:
            status_var.set(f"Failed at {result.finished_at.strftime('%H:%M:%S')}")
            self._log(f"{result.workflow.name} failed with exit code {result.returncode}.")

        if result.stdout.strip():
            self._log(result.stdout.strip())
        if result.stderr.strip():
            self._log(result.stderr.strip())

        self.busy_workflow_key = None
        self._set_run_buttons_state("normal")
        self.refresh_outputs()

    def _handle_run_error(self, workflow_key: str, message: str) -> None:
        workflow = WORKFLOWS[workflow_key]
        self.workflow_widgets[workflow_key]["status_var"].set("Run error")
        self._log(f"{workflow.name} crashed before completion: {message}")
        self.busy_workflow_key = None
        self._set_run_buttons_state("normal")

    def _set_run_buttons_state(self, state: str) -> None:
        for widgets in self.workflow_widgets.values():
            widgets["run_button"].configure(state=state)

    def _preview_latest_workflow_log(self) -> None:
        log_path = LATEST_DIR / self.selected_latest_workflow.get() / "run_log.txt"
        self._set_log_preview(log_path)

    def _preview_selected_run_log(self) -> None:
        selection = self.recent_runs_listbox.curselection()
        if not selection or not self.recent_run_paths:
            return
        timestamp_dir = self.recent_run_paths[selection[0]]
        workflow_key = self.selected_latest_workflow.get()
        candidate = timestamp_dir / workflow_key / "run_log.txt"
        if not candidate.exists():
            run_logs = list(timestamp_dir.glob("*/run_log.txt"))
            candidate = run_logs[0] if run_logs else candidate
        self._set_log_preview(candidate)

    def _set_log_preview(self, path: Path) -> None:
        if self.latest_log_text is None:
            return
        self.latest_log_text.configure(state="normal")
        self.latest_log_text.delete("1.0", "end")
        if path.exists():
            self.latest_log_text.insert("end", path.read_text(encoding="utf-8"))
        else:
            self.latest_log_text.insert("end", "No run summary yet for this workflow.")
        self.latest_log_text.configure(state="disabled")

    def _log(self, message: str) -> None:
        self.log_text.configure(state="normal")
        for line in str(message).splitlines():
            self.log_text.insert("end", f"[{self._now()}] {line}\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    @staticmethod
    def _now() -> str:
        return datetime.now().strftime("%H:%M:%S")


def run_self_check() -> int:
    ensure_runtime_dirs()
    print("Dashboard self-check")
    print(f"Base directory: {BASE_DIR}")
    print(f"Assets directory: {ASSETS_DIR}")
    print(f"Icon PNG: {ICON_PNG_PATH} ({ICON_PNG_PATH.exists()})")
    print(f"Icon ICO: {ICON_ICO_PATH} ({ICON_ICO_PATH.exists()})")
    print("Input folders:")
    for folder in INPUT_FOLDERS.values():
        print(f"  - {folder.label}: {folder.path}")
    print("Workflows:")
    for workflow in WORKFLOWS.values():
        print(f"  - {workflow.name}: {workflow.script_name}")
    print(f"Latest folder: {LATEST_DIR}")
    print(f"Runs folder: {RUNS_DIR}")
    return 0


def main() -> int:
    if "--self-check" in sys.argv:
        return run_self_check()
    app = InventoryDashboard()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
