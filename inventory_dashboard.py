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
    BASE_DIR,
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


class InventoryDashboard(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        ensure_runtime_dirs()
        self.title("AZCS Inventory Dashboard")
        self.geometry("1320x860")
        self.minsize(1120, 760)

        self.event_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self.busy_workflow_key: str | None = None

        self.input_widgets: dict[str, dict[str, object]] = {}
        self.workflow_widgets: dict[str, dict[str, object]] = {}
        self.latest_output_paths: list[Path] = []
        self.recent_run_paths: list[Path] = []

        self.selected_latest_workflow = tk.StringVar(value=next(iter(WORKFLOWS)))

        self._configure_style()
        self._build_layout()
        self.refresh_all()
        self.after(250, self._poll_events)

    def _configure_style(self) -> None:
        style = ttk.Style(self)
        try:
            style.theme_use("vista")
        except tk.TclError:
            pass
        style.configure("Title.TLabel", font=("Segoe UI", 15, "bold"))
        style.configure("Section.TLabelframe.Label", font=("Segoe UI", 10, "bold"))
        style.configure("Info.TLabel", foreground="#444444")
        style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"))

    def _build_layout(self) -> None:
        container = ttk.Frame(self, padding=12)
        container.pack(fill="both", expand=True)

        header = ttk.Frame(container)
        header.pack(fill="x", pady=(0, 10))
        ttk.Label(header, text="AZCS Inventory Dashboard", style="Title.TLabel").pack(anchor="w")
        ttk.Label(
            header,
            text="Upload files, run workflows, and open the latest outputs from one place.",
            style="Info.TLabel",
        ).pack(anchor="w", pady=(2, 0))

        notebook = ttk.Notebook(container)
        notebook.pack(fill="both", expand=True)

        self.home_tab = ttk.Frame(notebook, padding=12)
        self.uploads_tab = ttk.Frame(notebook, padding=12)
        self.workflows_tab = ttk.Frame(notebook, padding=12)
        self.outputs_tab = ttk.Frame(notebook, padding=12)

        notebook.add(self.home_tab, text="Home")
        notebook.add(self.uploads_tab, text="Uploads")
        notebook.add(self.workflows_tab, text="Workflows")
        notebook.add(self.outputs_tab, text="Outputs")

        self._build_home_tab()
        self._build_uploads_tab()
        self._build_workflows_tab()
        self._build_outputs_tab()

        log_frame = ttk.LabelFrame(container, text="Run Log", padding=10, style="Section.TLabelframe")
        log_frame.pack(fill="both", expand=False, pady=(10, 0))
        self.log_text = ScrolledText(log_frame, height=10, wrap="word", font=("Consolas", 10))
        self.log_text.pack(fill="both", expand=True)
        self.log_text.insert("end", f"[{self._now()}] Dashboard ready.\n")
        self.log_text.configure(state="disabled")

    def _build_home_tab(self) -> None:
        summary = ttk.LabelFrame(self.home_tab, text="Overview", padding=12, style="Section.TLabelframe")
        summary.pack(fill="x")

        overview_lines = [
            "Use Uploads to route new files into the right input folders.",
            "Use Workflows to run the catalog, pricing, sales-match, receiving, and stock tools.",
            "Use Outputs to open the latest files copied into clean workflow-specific folders.",
            "Each successful run is archived under runs/<timestamp>/<workflow> and mirrored into latest/<workflow>.",
        ]
        for line in overview_lines:
            ttk.Label(summary, text=line, wraplength=1040, justify="left").pack(anchor="w", pady=2)

        quick = ttk.LabelFrame(self.home_tab, text="Quick Open", padding=12, style="Section.TLabelframe")
        quick.pack(fill="x", pady=(12, 0))

        quick_buttons = [
            ("Open Repo Folder", BASE_DIR),
            ("Open Inputs Folder", BASE_DIR / "inputs"),
            ("Open Outputs Folder", BASE_DIR / "outputs"),
            ("Open Latest Folder", LATEST_DIR),
            ("Open Runs Folder", RUNS_DIR),
        ]
        row = ttk.Frame(quick)
        row.pack(fill="x")
        for label, path in quick_buttons:
            ttk.Button(row, text=label, command=lambda target=path: self._open_path(target)).pack(side="left", padx=(0, 8))

    def _build_uploads_tab(self) -> None:
        intro = ttk.Label(
            self.uploads_tab,
            text="Add new files here. The dashboard copies them into the correct input folders for the scripts.",
            wraplength=1040,
            justify="left",
        )
        intro.pack(anchor="w", pady=(0, 10))

        canvas = tk.Canvas(self.uploads_tab, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self.uploads_tab, orient="vertical", command=canvas.yview)
        scroll_frame = ttk.Frame(canvas)

        scroll_frame.bind(
            "<Configure>",
            lambda event: canvas.configure(scrollregion=canvas.bbox("all")),
        )
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        for folder in INPUT_FOLDERS.values():
            frame = ttk.LabelFrame(scroll_frame, text=folder.label, padding=10, style="Section.TLabelframe")
            frame.pack(fill="x", pady=(0, 10))

            top = ttk.Frame(frame)
            top.pack(fill="x")
            ttk.Label(top, text=folder.description, wraplength=760, justify="left").pack(side="left", fill="x", expand=True)

            actions = ttk.Frame(top)
            actions.pack(side="right")
            ttk.Button(actions, text="Add Files", command=lambda key=folder.key: self._add_files(key)).pack(side="left", padx=(0, 6))
            ttk.Button(actions, text="Open Folder", command=lambda path=folder.path: self._open_path(path)).pack(side="left", padx=(0, 6))
            ttk.Button(actions, text="Refresh", command=lambda key=folder.key: self.refresh_input_folder(key)).pack(side="left")

            meta = ttk.Frame(frame)
            meta.pack(fill="x", pady=(8, 6))
            count_var = tk.StringVar(value="0 files")
            ttk.Label(meta, textvariable=count_var, style="Info.TLabel").pack(side="left")
            ttk.Label(meta, text=str(folder.path), style="Info.TLabel").pack(side="right")

            listbox = tk.Listbox(frame, height=4)
            listbox.pack(fill="x", expand=True)
            listbox.bind("<Double-Button-1>", lambda event, key=folder.key: self._open_selected_input_file(key))

            button_row = ttk.Frame(frame)
            button_row.pack(fill="x", pady=(6, 0))
            ttk.Button(button_row, text="Open Selected File", command=lambda key=folder.key: self._open_selected_input_file(key)).pack(side="left")

            self.input_widgets[folder.key] = {
                "count_var": count_var,
                "listbox": listbox,
                "paths": [],
            }

    def _build_workflows_tab(self) -> None:
        intro = ttk.Label(
            self.workflows_tab,
            text="Run one workflow at a time. The dashboard copies successful outputs into latest/<workflow> and archives each run under runs/<timestamp>/<workflow>.",
            wraplength=1040,
            justify="left",
        )
        intro.pack(anchor="w", pady=(0, 10))

        for workflow in WORKFLOWS.values():
            frame = ttk.LabelFrame(self.workflows_tab, text=workflow.name, padding=10, style="Section.TLabelframe")
            frame.pack(fill="x", pady=(0, 10))

            ttk.Label(frame, text=workflow.description, wraplength=1040, justify="left").pack(anchor="w")

            input_labels = ", ".join(INPUT_FOLDERS[key].label for key in workflow.input_keys)
            ttk.Label(frame, text=f"Inputs: {input_labels}", style="Info.TLabel").pack(anchor="w", pady=(4, 0))
            ttk.Label(frame, text=f"Script: {workflow.script_name}", style="Info.TLabel").pack(anchor="w")

            controls = ttk.Frame(frame)
            controls.pack(fill="x", pady=(8, 0))
            status_var = tk.StringVar(value="Ready.")
            run_button = ttk.Button(
                controls,
                text="Run Workflow",
                style="Accent.TButton",
                command=lambda key=workflow.key: self._run_workflow_async(key),
            )
            run_button.pack(side="left", padx=(0, 8))
            ttk.Button(controls, text="Open Latest Folder", command=lambda key=workflow.key: self._open_path(LATEST_DIR / key)).pack(side="left", padx=(0, 8))
            ttk.Button(controls, text="Open Runs Folder", command=lambda: self._open_path(RUNS_DIR)).pack(side="left")
            ttk.Label(controls, textvariable=status_var, style="Info.TLabel").pack(side="right")

            self.workflow_widgets[workflow.key] = {
                "status_var": status_var,
                "run_button": run_button,
            }

    def _build_outputs_tab(self) -> None:
        controls = ttk.Frame(self.outputs_tab)
        controls.pack(fill="x")
        ttk.Label(controls, text="Workflow:").pack(side="left")
        selector = ttk.Combobox(
            controls,
            values=[workflow.name for workflow in WORKFLOWS.values()],
            state="readonly",
            width=28,
        )
        selector.pack(side="left", padx=(8, 8))
        selector.current(0)
        selector.bind("<<ComboboxSelected>>", lambda event: self._on_latest_workflow_change(selector.current()))
        self.latest_selector = selector

        ttk.Button(controls, text="Refresh", command=self.refresh_outputs).pack(side="left", padx=(0, 8))
        ttk.Button(controls, text="Open Latest Folder", command=self._open_current_latest_folder).pack(side="left", padx=(0, 8))
        ttk.Button(controls, text="Open Runs Folder", command=lambda: self._open_path(RUNS_DIR)).pack(side="left")

        panes = ttk.Panedwindow(self.outputs_tab, orient="horizontal")
        panes.pack(fill="both", expand=True, pady=(10, 0))

        latest_frame = ttk.LabelFrame(panes, text="Latest Files", padding=10, style="Section.TLabelframe")
        runs_frame = ttk.LabelFrame(panes, text="Recent Runs", padding=10, style="Section.TLabelframe")
        panes.add(latest_frame, weight=2)
        panes.add(runs_frame, weight=1)

        self.latest_listbox = tk.Listbox(latest_frame)
        self.latest_listbox.pack(fill="both", expand=True)
        self.latest_listbox.bind("<Double-Button-1>", lambda event: self._open_selected_latest_file())

        latest_buttons = ttk.Frame(latest_frame)
        latest_buttons.pack(fill="x", pady=(8, 0))
        ttk.Button(latest_buttons, text="Open Selected File", command=self._open_selected_latest_file).pack(side="left")

        self.recent_runs_listbox = tk.Listbox(runs_frame)
        self.recent_runs_listbox.pack(fill="both", expand=True)
        self.recent_runs_listbox.bind("<Double-Button-1>", lambda event: self._open_selected_run_folder())

        runs_buttons = ttk.Frame(runs_frame)
        runs_buttons.pack(fill="x", pady=(8, 0))
        ttk.Button(runs_buttons, text="Open Selected Run Folder", command=self._open_selected_run_folder).pack(side="left")

    def refresh_all(self) -> None:
        for key in INPUT_FOLDERS:
            self.refresh_input_folder(key)
        self.refresh_outputs()

    def refresh_input_folder(self, folder_key: str) -> None:
        files = list_input_files(folder_key)
        widgets = self.input_widgets[folder_key]
        listbox = widgets["listbox"]
        count_var = widgets["count_var"]

        listbox.delete(0, "end")
        display_lines = []
        for path in files:
            modified = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            display_lines.append(f"{path.name}    ({modified})")
        for line in display_lines:
            listbox.insert("end", line)

        widgets["paths"] = files
        count_var.set(f"{len(files)} files")

    def refresh_outputs(self) -> None:
        self._refresh_latest_files()
        self._refresh_recent_runs()

    def _refresh_latest_files(self) -> None:
        workflow_key = list(WORKFLOWS.keys())[self.latest_selector.current()]
        self.selected_latest_workflow.set(workflow_key)
        self.latest_output_paths = list_latest_outputs(workflow_key)
        self.latest_listbox.delete(0, "end")
        for path in self.latest_output_paths:
            modified = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            self.latest_listbox.insert("end", f"{path.name}    ({modified})")

    def _refresh_recent_runs(self) -> None:
        self.recent_run_paths = list_recent_runs(limit=20)
        self.recent_runs_listbox.delete(0, "end")
        for path in self.recent_run_paths:
            self.recent_runs_listbox.insert("end", path.name)

    def _on_latest_workflow_change(self, index: int) -> None:
        workflow_key = list(WORKFLOWS.keys())[index]
        self.selected_latest_workflow.set(workflow_key)
        self._refresh_latest_files()

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
        if not selection:
            messagebox.showinfo("Open File", "Select a file first.")
            return
        path = widgets["paths"][selection[0]]
        self._open_path(path)

    def _open_current_latest_folder(self) -> None:
        workflow_key = self.selected_latest_workflow.get()
        self._open_path(LATEST_DIR / workflow_key)

    def _open_selected_latest_file(self) -> None:
        selection = self.latest_listbox.curselection()
        if not selection:
            messagebox.showinfo("Open File", "Select a latest output file first.")
            return
        self._open_path(self.latest_output_paths[selection[0]])

    def _open_selected_run_folder(self) -> None:
        selection = self.recent_runs_listbox.curselection()
        if not selection:
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
        self.workflow_widgets[workflow_key]["status_var"].set("Running...")
        self._log(f"Starting workflow: {workflow.name}")

        worker = threading.Thread(target=self._run_workflow_worker, args=(workflow_key,), daemon=True)
        worker.start()

    def _run_workflow_worker(self, workflow_key: str) -> None:
        try:
            result = run_workflow(workflow_key)
            self.event_queue.put(("run_result", result))
        except Exception as exc:  # pragma: no cover - GUI guardrail
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
            status_var.set(f"Last run succeeded at {result.finished_at.strftime('%H:%M:%S')}")
            self._log(
                f"{result.workflow.name} finished successfully. "
                f"Copied {len(result.copied_outputs)} output file(s) into {result.latest_dir}."
            )
            if result.missing_outputs:
                self._log(f"Missing expected outputs: {', '.join(result.missing_outputs)}")
        else:
            status_var.set(f"Last run failed at {result.finished_at.strftime('%H:%M:%S')}")
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
