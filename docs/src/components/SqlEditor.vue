<template>
  <div class="sql-editor-container">
    <div class="editor-header">
      <span class="title">DataVo WASM SQL Playground</span>
      <div class="header-actions">
        <span class="shortcut-hint">⌘/Ctrl + Enter</span>
        <button
          @click="resetPlayground"
          :disabled="isRunning"
          class="reset-btn"
        >
          Reset Playground
        </button>
        <button @click="executeSql" :disabled="isRunning" class="execute-btn">
          {{ isRunning ? "Executing..." : "Execute SQL" }}
        </button>
      </div>
    </div>

    <div class="code-area">
      <div ref="editorHost" class="sql-editor"></div>
    </div>

    <div v-if="results.length > 0" class="results-area">
      <div v-for="(res, index) in results" :key="index" class="result-block">
        <div v-if="res.IsError" class="error-msg">
          <strong>Error:</strong> {{ res.Messages?.join(", ") }}
          <div v-if="res.ErrorLine && res.ErrorColumn" class="error-location">
            At line {{ res.ErrorLine }}, column {{ res.ErrorColumn }}
          </div>
        </div>

        <div v-else>
          <div class="success-msg">
            {{ res.Messages?.join(", ") || "OK" }}
          </div>

          <div
            class="table-container"
            v-if="
              res.Data &&
              res.Data.length > 0 &&
              res.Fields &&
              res.Fields.length > 0
            "
          >
            <table class="results-table">
              <thead>
                <tr>
                  <th v-for="field in res.Fields" :key="field">
                    {{ field }}
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, rIdx) in res.Data" :key="rIdx">
                  <td v-for="field in res.Fields" :key="field">
                    {{
                      row[field] !== null && row[field] !== undefined
                        ? row[field]
                        : "NULL"
                    }}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <div v-else-if="hasExecuted" class="results-area">
      <div class="success-msg">
        Command executed successfully with no output.
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount } from "vue";
import {
  EditorState,
  StateEffect,
  StateField,
  Range,
} from "@codemirror/state";
import {
  EditorView,
  Decoration,
  keymap,
  lineNumbers,
  highlightActiveLine,
} from "@codemirror/view";
import {
  defaultKeymap,
  history,
  historyKeymap,
  indentWithTab,
} from "@codemirror/commands";
import {
  bracketMatching,
  HighlightStyle,
  defaultHighlightStyle,
  syntaxHighlighting,
} from "@codemirror/language";
import { sql } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { tags as t } from "@lezer/highlight";
import { DataVoClient, QueryResult } from "../DataVoClient";

const props = defineProps({
  initialQuery: {
    type: String,
    default:
      "CREATE DATABASE Playground;\nUSE Playground;\nCREATE TABLE Users (Id INT, Name VARCHAR, Age INT);\nINSERT INTO Users VALUES (1, 'Alice', 30), (2, 'Bob', 25);\nSELECT * FROM Users;",
  },
});

const sqlQuery = ref(props.initialQuery);
const results = ref<QueryResult[]>([]);
const isRunning = ref(false);
const hasExecuted = ref(false);
const editorHost = ref<HTMLDivElement | null>(null);

let editorView: EditorView | null = null;

type ErrorMarkerPayload = { from: number; to: number };

const setErrorMarkerEffect = StateEffect.define<ErrorMarkerPayload>();
const clearErrorMarkerEffect = StateEffect.define<void>();

const errorMarkerDecoration = Decoration.mark({ class: "cm-error-squiggle" });

const errorMarkerField = StateField.define({
  create() {
    return Decoration.none;
  },
  update(decorations, tr) {
    decorations = decorations.map(tr.changes);

    for (const effect of tr.effects) {
      if (effect.is(clearErrorMarkerEffect)) {
        decorations = Decoration.none;
      }
      if (effect.is(setErrorMarkerEffect)) {
        decorations = Decoration.set([
          errorMarkerDecoration.range(effect.value.from, effect.value.to),
        ] as Range<Decoration>[]);
      }
    }

    return decorations;
  },
  provide: (f) => EditorView.decorations.from(f),
});

const executeShortcut = EditorView.domEventHandlers({
  keydown(event, view) {
    if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
      event.preventDefault();
      sqlQuery.value = view.state.doc.toString();
      void executeSql();
    }
  },
});

const syncSqlState = EditorView.updateListener.of((update) => {
  if (update.docChanged) {
    sqlQuery.value = update.state.doc.toString();
  }
});

const sqlHighlightStyle = HighlightStyle.define([
  { tag: [t.keyword, t.operatorKeyword], color: "#c084fc", fontWeight: "600" },
  {
    tag: [t.controlKeyword, t.moduleKeyword],
    color: "#c084fc",
    fontWeight: "700",
  },
  { tag: [t.string], color: "#86efac" },
  { tag: [t.number, t.integer, t.float], color: "#fbbf24" },
  { tag: [t.bool, t.null], color: "#f472b6", fontWeight: "600" },
  { tag: [t.typeName], color: "#38bdf8" },
  { tag: [t.comment], color: "#94a3b8", fontStyle: "italic" },
  { tag: [t.propertyName, t.attributeName], color: "#f9a8d4" },
  { tag: [t.variableName], color: "#e2e8f0" },
  { tag: [t.definitionKeyword], color: "#a78bfa", fontWeight: "700" },
]);

const createEditor = () => {
  if (!editorHost.value) {
    return;
  }

  const state = EditorState.create({
    doc: sqlQuery.value,
    extensions: [
      lineNumbers(),
      history(),
      bracketMatching(),
      highlightActiveLine(),
      syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
      syntaxHighlighting(sqlHighlightStyle),
      keymap.of([...defaultKeymap, ...historyKeymap, indentWithTab]),
      sql(),
      oneDark,
      syncSqlState,
      executeShortcut,
      errorMarkerField,
      EditorView.lineWrapping,
      EditorView.theme({
        "&": {
          fontSize: "13px",
          minHeight: "160px",
          backgroundColor: "var(--vp-code-block-bg)",
          color: "var(--vp-code-block-color)",
        },
        ".cm-scroller": {
          fontFamily: "var(--vp-font-family-mono)",
          minHeight: "160px",
        },
        ".cm-gutters": {
          backgroundColor: "var(--vp-code-block-bg)",
          color: "var(--vp-c-text-3)",
          borderRight: "1px solid var(--vp-c-divider)",
        },
        ".cm-activeLine": {
          backgroundColor: "rgba(255, 255, 255, 0.04)",
        },
        ".cm-activeLineGutter": {
          backgroundColor: "rgba(255, 255, 255, 0.04)",
        },
        ".cm-content": {
          padding: "1rem 0",
        },
      }),
    ],
  });

  editorView = new EditorView({
    state,
    parent: editorHost.value,
  });
};

const executeSql = async () => {
  if (editorView) {
    sqlQuery.value = editorView.state.doc.toString();
  }

  clearErrorMarker();

  isRunning.value = true;
  hasExecuted.value = false;
  results.value = [];

  try {
    const client = DataVoClient.getInstance();
    await client.initialize();

    const parsedResults = client.execute(sqlQuery.value);
    results.value = parsedResults;
    applyErrorMarkerFromResults(parsedResults);
    hasExecuted.value = true;
  } catch (e: any) {
    results.value = [
      {
        IsError: true,
        Messages: [e.message || "Failed to initialize or execute"],
        Data: [],
        Fields: [],
        ExecutionTime: "",
      },
    ];

    applyErrorMarkerFromResults(results.value);
  } finally {
    isRunning.value = false;
  }
};

const resetPlayground = async () => {
  if (isRunning.value) {
    return;
  }

  try {
    const client = DataVoClient.getInstance();
    await client.initialize();
    client.reset();
    results.value = [
      {
        IsError: false,
        Messages: ["Playground storage cleared."],
        Data: [],
        Fields: [],
        ExecutionTime: "",
      },
    ];
    clearErrorMarker();
    hasExecuted.value = true;
  } catch (e: any) {
    results.value = [
      {
        IsError: true,
        Messages: [e.message || "Failed to reset playground"],
        Data: [],
        Fields: [],
        ExecutionTime: "",
      },
    ];
    clearErrorMarker();
  }
};

const clearErrorMarker = () => {
  if (!editorView) {
    return;
  }

  editorView.dispatch({ effects: [clearErrorMarkerEffect.of()] });
};

const applyErrorMarkerFromResults = (queryResults: QueryResult[]) => {
  if (!editorView) {
    return;
  }

  const firstError = queryResults.find(
    (r) => r.IsError && r.ErrorLine && r.ErrorColumn,
  );

  if (!firstError || !firstError.ErrorLine || !firstError.ErrorColumn) {
    return;
  }

  const marker = toDocumentRange(firstError.ErrorLine, firstError.ErrorColumn);
  if (!marker) {
    return;
  }

  editorView.dispatch({
    effects: [setErrorMarkerEffect.of(marker)],
    selection: { anchor: marker.from },
    scrollIntoView: true,
  });
};

const toDocumentRange = (line: number, column: number): ErrorMarkerPayload | null => {
  if (!editorView) {
    return null;
  }

  const doc = editorView.state.doc;
  if (line < 1 || line > doc.lines) {
    return null;
  }

  const lineInfo = doc.line(line);
  const clampedColumn = Math.max(1, Math.min(column, lineInfo.length + 1));
  const from = lineInfo.from + clampedColumn - 1;
  const to = Math.min(from + 1, lineInfo.to);

  return {
    from,
    to: to > from ? to : from + 1,
  };
};

onMounted(() => {
  createEditor();
});

onBeforeUnmount(() => {
  editorView?.destroy();
  editorView = null;
});
</script>

<style scoped>
.sql-editor-container {
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  overflow: hidden;
  margin: 1rem 0;
  background-color: var(--vp-c-bg-soft);
}

.editor-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.5rem 1rem;
  background-color: var(--vp-c-bg-mute);
  border-bottom: 1px solid var(--vp-c-divider);
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.shortcut-hint {
  font-size: 0.75rem;
  color: var(--vp-c-text-3);
}

.title {
  font-weight: 600;
  font-size: 0.9em;
  color: var(--vp-c-text-1);
}

.execute-btn,
.reset-btn {
  background-color: var(--vp-button-brand-bg, var(--vp-c-brand-1));
  color: var(--vp-button-brand-text, white);
  border: 1px solid var(--vp-button-brand-border, transparent);
  padding: 0.4rem 1.2rem;
  border-radius: 6px;
  font-size: 0.9em;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s ease;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.execute-btn:hover:not(:disabled),
.reset-btn:hover:not(:disabled) {
  background-color: var(--vp-button-brand-hover-bg, var(--vp-c-brand-2));
  transform: translateY(-1px);
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.15);
}

.execute-btn:active:not(:disabled),
.reset-btn:active:not(:disabled) {
  background-color: var(--vp-button-brand-active-bg, var(--vp-c-brand-3));
  transform: translateY(0);
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.1);
}

.execute-btn:disabled,
.reset-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
  transform: none;
  box-shadow: none;
}

.code-area {
  padding: 0;
}

.sql-editor {
  width: 100%;
  min-height: 160px;
}

:deep(.cm-error-squiggle) {
  text-decoration-line: underline;
  text-decoration-style: wavy;
  text-decoration-color: var(--vp-c-danger-1);
  text-underline-offset: 2px;
  background-color: color-mix(in srgb, var(--vp-c-danger-1) 12%, transparent);
}

.results-area {
  border-top: 1px solid var(--vp-c-divider);
  padding: 1rem;
  background-color: var(--vp-c-bg);
  max-height: 400px;
  overflow-y: auto;
}

.result-block {
  margin-bottom: 1rem;
  padding-bottom: 1rem;
  border-bottom: 1px dashed var(--vp-c-divider);
}

.result-block:last-child {
  margin-bottom: 0;
  padding-bottom: 0;
  border-bottom: none;
}

.error-msg {
  color: var(--vp-c-danger-1);
  font-size: 0.9em;
  background-color: var(--vp-c-danger-soft);
  padding: 0.5rem;
  border-radius: 4px;
}

.error-location {
  margin-top: 0.35rem;
  font-size: 0.82em;
  color: var(--vp-c-danger-2);
}

.error-stack {
  margin-top: 0.5rem;
  font-size: 0.8em;
  white-space: pre-wrap;
  font-family: var(--vp-font-family-mono);
}

.success-msg {
  color: var(--vp-c-text-2);
  font-size: 0.85em;
  margin-bottom: 0.5rem;
}

.table-container {
  overflow-x: auto;
}

.results-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85em;
}

.results-table th,
.results-table td {
  border: 1px solid var(--vp-c-divider);
  padding: 0.4rem 0.6rem;
  text-align: left;
}

.results-table th {
  background-color: var(--vp-c-bg-mute);
  font-weight: 600;
}
</style>
