import { useEffect, useMemo, useRef, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
  DropdownMenuItem,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { CheckCircle2, Info, Loader2, Play, RotateCw, X, XCircle } from "lucide-react";
import { toast } from "sonner";

const API_BASE = `http://${window.location.hostname}:8000`;
const STATUS_OPTIONS = [
  "open",
  "resolved",
  "duplicate",
  "not_a_bug",
  "feature_request",
  "unclear",
] as const;
const STATUS_LABELS: Record<(typeof STATUS_OPTIONS)[number], string> = {
  open: "open",
  resolved: "resolved",
  duplicate: "duplicate",
  not_a_bug: "not a bug",
  feature_request: "feature request",
  unclear: "unclear",
};
const CONFIDENCE_OPTIONS = ["low", "medium", "high"] as const;
const CONFIDENCE_DOTS: Record<(typeof CONFIDENCE_OPTIONS)[number], string> = {
  low: "●○○",
  medium: "●●○",
  high: "●●●",
};
const MODEL_OPTIONS = [
  { value: "auto", label: "Auto" },
  { value: "pro", label: "Pro" },
  { value: "flash", label: "Flash" },
  { value: "flash-lite", label: "Flash Lite" },
  { value: "gemini-3-pro-preview", label: "Gemini 3 Pro (preview)" },
  { value: "gemini-3-flash-preview", label: "Gemini 3 Flash (preview)" },
  { value: "gemini-2.5-pro", label: "Gemini 2.5 Pro" },
  { value: "gemini-2.5-flash", label: "Gemini 2.5 Flash" },
  { value: "gemini-2.5-flash-lite", label: "Gemini 2.5 Flash Lite" },
];

const confidenceDots = (confidence?: string | null) => {
  if (!confidence) return "○○○";
  return CONFIDENCE_DOTS[confidence as (typeof CONFIDENCE_OPTIONS)[number]] ?? "○○○";
};

type QueueItem = {
  thread_id: string;
  title: string;
  url?: string;
  decision_status?: string | null;
  status_guess?: string | null;
  confidence?: string | null;
};

type Post = {
  post_id: string;
  author: string;
  posted_at: string;
  body_text: string;
};

type ThreadDetail = {
  thread: { thread_id: string; title: string; url?: string };
  posts: Post[];
  decision?: { status: string; duplicate_of?: string | null; notes?: string | null } | null;
  judgment?: {
    summary?: string | null;
    status_guess?: string | null;
    confidence?: string | null;
    evidence?: string | null;
    duplicates?: string | null;
  } | null;
};

type SearchResult = { thread_id: string; title: string };

const readAutoRun = () => sessionStorage.getItem("autoRun") === "true";

function App() {
  const [queue, setQueue] = useState<QueueItem[]>([]);
  const [queueTotal, setQueueTotal] = useState<number>(0);
  const [queueOffset, setQueueOffset] = useState<number>(0);
  const [selectedId, setSelectedId] = useState<string | null>(() => localStorage.getItem("selectedId"));
  const [detail, setDetail] = useState<ThreadDetail | null>(null);
  const [status, setStatus] = useState<string>("open");
  const [duplicateOf, setDuplicateOf] = useState<string>("");
  const [notes, setNotes] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [queueStatusGuess, setQueueStatusGuess] = useState<string>(
    () => localStorage.getItem("queueStatusGuess") || "all",
  );
  const [queueConfidence, setQueueConfidence] = useState<string>(
    () => localStorage.getItem("queueConfidence") || "all",
  );
  const [queueQuery, setQueueQuery] = useState<string>(() => localStorage.getItem("queueQuery") || "");
  const [debouncedQueueQuery, setDebouncedQueueQuery] = useState<string>("");
  const [queueScope, setQueueScope] = useState<string>(() => localStorage.getItem("queueScope") || "unreviewed");
  const [queueHasLlm, setQueueHasLlm] = useState<string>(() => localStorage.getItem("queueHasLlm") || "any");
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [saveMsg, setSaveMsg] = useState<string>("" );
  const judgingControllers = useRef<Map<string, AbortController>>(new Map());
  const selectedIdRef = useRef<string | null>(null);
  const bulkPollTimer = useRef<number | null>(null);
  const bulkPollInFlight = useRef<boolean>(false);
  const queueRefreshTimer = useRef<number | null>(null);
  const quotaFetchAtRef = useRef<number>(0);
  const [llmJobs, setLlmJobs] = useState<Record<string, { status: string; error?: string | null }>>({});
  const llmJobsRef = useRef<Record<string, { status: string; error?: string | null }>>({});
  const [llmMaxInflight, setLlmMaxInflight] = useState<number | null>(null);
  const [llmCapacityHint, setLlmCapacityHint] = useState<boolean>(false);
  const [llmModel, setLlmModel] = useState<string>(() => localStorage.getItem("llmModel") || "auto");
  const [bulkInfo, setBulkInfo] = useState<{ label: string; total: number; queued: number; running: boolean } | null>(
    null,
  );
  const bulkAbortRef = useRef<boolean>(false);
  const [autoRun, setAutoRun] = useState<boolean>(() => readAutoRun());
  const [autoRunNote, setAutoRunNote] = useState<string>("");
  const queueRef = useRef<QueueItem[]>([]);
  const queueFiltersRef = useRef({
    queueScope: "unreviewed",
    queueHasLlm: "any",
    queueStatusGuess: "all",
    queueConfidence: "all",
    debouncedQueueQuery: "",
  });

  const loadQueue = (
    reset: boolean = true,
    snapshot?: {
      queueScope: string;
      queueHasLlm: string;
      queueStatusGuess: string;
      queueConfidence: string;
      debouncedQueueQuery: string;
    },
  ) => {
    const filters = snapshot ?? queueFiltersRef.current;
    const limit = 50;
    const offset = reset ? 0 : queueOffset + limit;
    const params = new URLSearchParams({
      status: filters.queueScope,
      limit: String(limit),
      offset: String(offset),
    });
    if (filters.queueHasLlm === "yes" && filters.queueStatusGuess !== "all") {
      params.set("status_guess", filters.queueStatusGuess);
    }
    if (filters.queueHasLlm === "yes" && filters.queueConfidence !== "all") {
      params.set("confidence", filters.queueConfidence);
    }
    if (filters.debouncedQueueQuery.trim()) params.set("q", filters.debouncedQueueQuery.trim());
    if (filters.queueHasLlm !== "any") {
      params.set("has_llm", filters.queueHasLlm === "yes" ? "true" : "false");
    }
    fetch(`${API_BASE}/queue?${params.toString()}`)
      .then((r) => r.json())
      .then((data) => {
        const items = data.items ?? [];
        setQueueTotal(data.total ?? items.length);
        if (reset) {
          setQueue(items);
          setQueueOffset(0);
          if (items.length) {
            const stored = localStorage.getItem("selectedId");
            const nextId =
              (stored && items.some((item: QueueItem) => item.thread_id === stored) && stored) ||
              selectedId ||
              items[0].thread_id;
            if (nextId && nextId !== selectedId) {
              setSelectedId(nextId);
            }
          }
        } else {
          setQueue((prev) => [...prev, ...items]);
          setQueueOffset(offset);
        }
      });
  };

  useEffect(() => {
    const handle = setTimeout(() => {
      setDebouncedQueueQuery(queueQuery);
    }, 300);
    return () => clearTimeout(handle);
  }, [queueQuery]);

  useEffect(() => {
    localStorage.setItem("queueQuery", queueQuery);
  }, [queueQuery]);

  useEffect(() => {
    if (queueHasLlm !== "yes") {
      setQueueStatusGuess("all");
      setQueueConfidence("all");
    }
  }, [queueHasLlm]);

  useEffect(() => {
    queueRef.current = queue;
  }, [queue]);

  useEffect(() => {
    queueFiltersRef.current = {
      queueScope,
      queueHasLlm,
      queueStatusGuess,
      queueConfidence,
      debouncedQueueQuery,
    };
    localStorage.setItem("queueScope", queueScope);
    localStorage.setItem("queueHasLlm", queueHasLlm);
    localStorage.setItem("queueStatusGuess", queueStatusGuess);
    localStorage.setItem("queueConfidence", queueConfidence);
    loadQueue(true);
  }, [queueScope, queueHasLlm, queueStatusGuess, queueConfidence, debouncedQueueQuery]);

  useEffect(() => {
    if (!selectedId) return;
    selectedIdRef.current = selectedId;
    setLoading(true);
    fetch(`${API_BASE}/thread/${selectedId}`)
      .then((r) => r.json())
      .then((data: ThreadDetail) => {
        setDetail(data);
        const d = data.decision;
        if (d?.status) {
          setStatus(d.status);
        } else if (data.judgment?.status_guess) {
          setStatus(data.judgment.status_guess);
        } else {
          setStatus("open");
        }
        setDuplicateOf(d?.duplicate_of ?? "");
        setNotes(d?.notes ?? "");
      })
      .finally(() => setLoading(false));
  }, [selectedId]);

  useEffect(() => {
    selectedIdRef.current = selectedId;
  }, [selectedId]);

  useEffect(() => {
    fetch(`${API_BASE}/judge/active`)
      .then((r) => r.json())
      .then((data) => {
        const items = data.items ?? [];
        if (!items.length) return;
        setLlmJobs((prev) => {
          const next = { ...prev };
          for (const item of items) {
            next[item.thread_id] = { status: item.status, error: item.error ?? null };
          }
          return next;
        });
      })
      .catch(() => null);
  }, []);

  useEffect(() => {
    if (!searchQuery.trim()) {
      setSearchResults([]);
      return;
    }
    const params = new URLSearchParams({ q: searchQuery.trim(), limit: "10" });
    fetch(`${API_BASE}/search?${params.toString()}`)
      .then((r) => r.json())
      .then((data) => setSearchResults(data));
  }, [searchQuery]);

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      const tag = target?.tagName?.toLowerCase();
      if (tag === "input" || tag === "textarea" || target?.isContentEditable) return;
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "u") {
        if (detail?.judgment?.status_guess && status !== detail.judgment.status_guess) {
          event.preventDefault();
          applySuggestion();
        }
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [detail?.judgment?.status_guess]);

  useEffect(() => {
    llmJobsRef.current = llmJobs;
  }, [llmJobs]);

  useEffect(() => {
    localStorage.setItem("llmModel", llmModel);
  }, [llmModel]);

  useEffect(() => {
    sessionStorage.setItem("autoRun", autoRun ? "true" : "false");
  }, [autoRun]);

  const queueIndex = useMemo(() => {
    return queue.findIndex((q) => q.thread_id === selectedId);
  }, [queue, selectedId]);
  const modelLabel = useMemo(() => {
    return MODEL_OPTIONS.find((model) => model.value === llmModel)?.label ?? llmModel;
  }, [llmModel]);

  const parseTime = (value?: string) => {
    if (!value) return null;
    const direct = Date.parse(value);
    if (!Number.isNaN(direct)) return direct;
    const trimmed = value.replace(/\.\d+/, "");
    const withZ = trimmed.endsWith("Z") ? trimmed : `${trimmed}Z`;
    const fallback = Date.parse(withZ);
    return Number.isNaN(fallback) ? null : fallback;
  };

  const formatQuotaNote = (resetAt?: string, exhaustedAt?: string) => {
    let agoLabel = "";
    if (exhaustedAt) {
      const exhaustedMs = parseTime(exhaustedAt);
      if (exhaustedMs) {
        const agoMs = Date.now() - exhaustedMs;
        if (agoMs >= 0) {
          const agoMinutes = Math.max(0, Math.round(agoMs / 60000));
          const hours = Math.floor(agoMinutes / 60);
          const minutes = agoMinutes % 60;
          agoLabel = hours > 0 ? `${hours}h${minutes}m` : `${minutes}m`;
        }
      }
    }
    const agoSuffix = agoLabel ? ` (${agoLabel} ago)` : "";
    if (!resetAt) return `Quota exhausted for ${modelLabel}${agoSuffix} · runs might fail`;
    const resetMs = parseTime(resetAt);
    if (resetMs) {
      const resetDate = new Date(resetMs);
      const now = new Date();
      const sameDay = resetDate.toDateString() === now.toDateString();
      const timeLabel = resetDate.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
      const dateLabel = resetDate.toLocaleDateString([], { month: "short", day: "numeric" });
      const untilLabel = sameDay ? timeLabel : `${dateLabel} ${timeLabel}`;
      return `Quota exhausted for ${modelLabel} · until ${untilLabel} · runs might fail`;
    }
    return `Quota exhausted for ${modelLabel}${agoSuffix} · runs might fail`;
  };

  const isRecentQuota = (at?: string) => {
    if (!at) return false;
    const atMs = parseTime(at);
    if (!atMs) return false;
    const ageMs = Date.now() - atMs;
    return ageMs >= 0 && ageMs <= 6 * 60 * 60 * 1000;
  };

  const fetchQuotaState = (force: boolean = false) => {
    const now = Date.now();
    if (!force && now - quotaFetchAtRef.current < 30000) return;
    quotaFetchAtRef.current = now;
    fetch(`${API_BASE}/judge/state?model=${encodeURIComponent(llmModel)}`)
      .then((r) => r.json())
      .then((data) => {
        const state = data.state ?? {};
        const quotaMsg = state[`quota_exhausted_message:${llmModel}`] as string | undefined;
        const resetAt = state[`quota_reset_at:${llmModel}`] as string | undefined;
        const exhaustedAt = state[`quota_exhausted_at:${llmModel}`] as string | undefined;
        if (quotaMsg && isRecentQuota(exhaustedAt)) {
          setAutoRunNote(formatQuotaNote(resetAt, exhaustedAt));
        } else {
          setAutoRunNote("");
        }
      })
      .catch(() => null);
  };

  useEffect(() => {
    fetchQuotaState(true);
  }, [llmModel, modelLabel]);

  useEffect(() => {
    if (selectedId) {
      localStorage.setItem("selectedId", selectedId);
    }
  }, [selectedId]);

  const saveDecision = async () => {
    if (!selectedId) return;
    const res = await fetch(`${API_BASE}/decision`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        thread_id: selectedId,
        status,
        duplicate_of: duplicateOf || null,
        notes: notes || null,
      }),
    });
    if (res.ok) {
      setDetail((prev) =>
        prev
          ? {
              ...prev,
              decision: {
                status,
                duplicate_of: duplicateOf || null,
                notes: notes || null,
              },
            }
          : prev,
      );
      setQueue((prev) =>
        prev.map((item) =>
          item.thread_id === selectedId ? { ...item, decision_status: status } : item,
        ),
      );
      setSaveMsg("Saved");
      setTimeout(() => setSaveMsg(""), 1500);
      loadQueue(true);
    } else {
      setSaveMsg("Save failed");
      setTimeout(() => setSaveMsg(""), 2000);
    }
  };

  const saveAndNext = async () => {
    await saveDecision();
    loadQueue(true);
    if (queueIndex >= 0 && queueIndex < queue.length - 1) {
      setSelectedId(queue[queueIndex + 1].thread_id);
    }
  };

  const scheduleQueueRefresh = () => {
    if (queueRefreshTimer.current) return;
    const snapshot = { ...queueFiltersRef.current };
    queueRefreshTimer.current = window.setTimeout(() => {
      queueRefreshTimer.current = null;
      loadQueue(true, snapshot);
    }, 600);
  };

  const pollAllStatus = async () => {
    if (bulkPollInFlight.current) return;
    const activeIds = Object.entries(llmJobsRef.current)
      .filter(([, job]) => job.status === "queued" || job.status === "running" || job.status === "starting")
      .map(([id]) => id);
    if (!activeIds.length) return;
    bulkPollInFlight.current = true;
    try {
      const res = await fetch(`${API_BASE}/judge/status/bulk`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ thread_ids: activeIds }),
      });
      if (!res.ok) return;
      const payload = await res.json().catch(() => ({}));
      const items: Array<{ thread_id: string; status: string; error?: string | null }> = payload.items ?? [];
      let shouldRefreshSelected = false;
      let anyDone = false;
      setLlmJobs((prev) => {
        const next = { ...prev };
        for (const item of items) {
          if (!item.thread_id) continue;
          next[item.thread_id] = { status: item.status, error: item.error ?? null };
          if (item.status === "done") {
            anyDone = true;
            if (item.thread_id === selectedIdRef.current) {
              shouldRefreshSelected = true;
            }
          }
        }
        return next;
      });
      if (anyDone) {
        scheduleQueueRefresh();
        fetchQuotaState();
      }
      if (shouldRefreshSelected && selectedIdRef.current) {
        const refreshed = await fetch(`${API_BASE}/thread/${selectedIdRef.current}`).then((r) => r.json());
        setDetail(refreshed);
        if (refreshed?.judgment?.status_guess) {
          setStatus(refreshed.judgment.status_guess);
        }
      }
    } finally {
      bulkPollInFlight.current = false;
    }
  };

  const runJudge = async (threadId?: string | null, modelOverride?: string) => {
    const id = threadId ?? selectedId;
    if (!id || ["queued", "running", "starting"].includes(llmJobs[id]?.status || "")) return;
    const model = modelOverride || llmModel;
    const controller = new AbortController();
    judgingControllers.current.set(id, controller);
    setLlmJobs((prev) => ({ ...prev, [id]: { status: "queued" } }));
    try {
      const params = new URLSearchParams({ model });
      const res = await fetch(`${API_BASE}/judge/${id}?${params.toString()}`, {
        method: "POST",
        signal: controller.signal,
      });
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        const msg = payload?.detail || `LLM failed (${res.status})`;
        setLlmJobs((prev) => ({ ...prev, [id]: { status: "error", error: msg } }));
        return;
      }
      const payload = await res.json().catch(() => ({}));
      const nextStatus = payload?.status || "queued";
      if (payload?.max_inflight && typeof payload.max_inflight === "number") {
        setLlmMaxInflight(payload.max_inflight);
      }
      if (payload?.queued_reason === "capacity") {
        setLlmCapacityHint(true);
      }
      setLlmJobs((prev) => ({ ...prev, [id]: { status: nextStatus } }));
    } catch (err: unknown) {
      if (err instanceof DOMException && err.name === "AbortError") {
        setLlmJobs((prev) => ({ ...prev, [id]: { status: "cancelled" } }));
        return;
      }
      setLlmJobs((prev) => ({ ...prev, [id]: { status: "error", error: "Request failed" } }));
    }
  };

  const runJudgeBatch = async (ids: string[], label: string) => {
    if (!ids.length) return;
    bulkAbortRef.current = false;
    setBulkInfo({ label, total: ids.length, queued: 0, running: true });
    let queued = 0;
    for (const id of ids) {
      if (bulkAbortRef.current) break;
      await runJudge(id, llmModel);
      queued += 1;
      setBulkInfo((prev) => (prev ? { ...prev, queued, running: true } : prev));
    }
    setBulkInfo((prev) => (prev ? { ...prev, queued, running: false } : prev));
  };

  const cancelBatch = () => {
    bulkAbortRef.current = true;
    setBulkInfo((prev) => (prev ? { ...prev, running: false } : prev));
  };

  const cancelAllRuns = () => {
    bulkAbortRef.current = true;
    setAutoRun(false);
    const targets = Object.entries(llmJobsRef.current)
      .filter(([, job]) => ["queued", "running", "starting"].includes(job.status))
      .map(([id]) => id);
    targets.forEach((id) => cancelJudge(id));
  };

  const cancelJudge = async (threadId?: string | null) => {
    const id = threadId ?? selectedId;
    if (!id) return;
    const controller = judgingControllers.current.get(id);
    if (controller) controller.abort();
    judgingControllers.current.delete(id);
    setLlmJobs((prev) => ({ ...prev, [id]: { status: "cancelled" } }));
    try {
      const res = await fetch(`${API_BASE}/judge/cancel/${id}`, { method: "POST" });
      const data = await res.json().catch(() => ({}));
      const status = data?.status || "cancelled";
      if (status === "cancelled") {
        setLlmJobs((prev) => ({ ...prev, [id]: { status: "cancelled" } }));
      } else if (status === "done") {
        setLlmJobs((prev) => ({ ...prev, [id]: { status: "done" } }));
      } else if (status === "error") {
        setLlmJobs((prev) => ({
          ...prev,
          [id]: { status: "error", error: data?.error || "Unknown error" },
        }));
      }
    } catch {
      setLlmJobs((prev) => ({
        ...prev,
        [id]: { status: "error", error: "Cancel failed" },
      }));
    }
  };

  const applySuggestion = () => {
    if (!detail?.judgment?.status_guess) return;
    setStatus(detail.judgment.status_guess);
  };

  const isLlmActive = (threadId?: string | null) => {
    if (!threadId) return false;
    const status = llmJobs[threadId]?.status;
    return status === "queued" || status === "running" || status === "starting";
  };

  const llmJobStatus = (threadId?: string | null) => {
    if (!threadId) return null;
    return llmJobs[threadId]?.status ?? null;
  };

  const jobEntries = Object.entries(llmJobs);
  const jobCounts = jobEntries.reduce(
    (acc, [, job]) => {
      acc.total += 1;
      acc[job.status] = (acc[job.status] || 0) + 1;
      return acc;
    },
    { total: 0 } as Record<string, number>,
  );
  const completedCount =
    (jobCounts.done || 0) + (jobCounts.error || 0) + (jobCounts.cancelled || 0) + (jobCounts.skipped || 0);
  const progress = jobCounts.total ? Math.round((completedCount / jobCounts.total) * 100) : 0;
  const activeCount =
    (jobCounts.queued || 0) + (jobCounts.running || 0) + (jobCounts.starting || 0);
  const clearCompleted = () => {
    setLlmJobs((prev) => {
      const next: Record<string, { status: string; error?: string | null }> = {};
      for (const [id, job] of Object.entries(prev)) {
        if (["done", "error", "cancelled", "idle", "skipped"].includes(job.status)) continue;
        next[id] = job;
      }
      return next;
    });
  };

  useEffect(() => {
    const active = (jobCounts.queued || 0) + (jobCounts.running || 0) + (jobCounts.starting || 0);
    if (active === 0 && llmCapacityHint) {
      setLlmCapacityHint(false);
    }
  }, [jobCounts.queued, jobCounts.running, jobCounts.starting, llmCapacityHint]);

  useEffect(() => {
    const active = (jobCounts.queued || 0) + (jobCounts.running || 0) + (jobCounts.starting || 0);
    if (bulkPollTimer.current) {
      window.clearInterval(bulkPollTimer.current);
      bulkPollTimer.current = null;
    }
    if (active === 0) return;
    const nextInterval = active > 20 ? 4000 : active > 10 ? 3000 : 2000;
    bulkPollTimer.current = window.setInterval(() => {
      void pollAllStatus();
    }, nextInterval);
    return () => {
      if (bulkPollTimer.current) {
        window.clearInterval(bulkPollTimer.current);
        bulkPollTimer.current = null;
      }
    };
  }, [jobCounts.queued, jobCounts.running, jobCounts.starting]);

  useEffect(() => {
    if (!autoRun) return;
    if (bulkInfo?.running) return;
    const noteFromErrors = Object.values(llmJobs).find(
      (job) => job.status === "error" && job.error && job.error.toLowerCase().includes("quota"),
    );
    if (noteFromErrors) {
      setAutoRun(false);
      fetchQuotaState(true);
      setBulkInfo(null);
      return;
    }
    const cap = llmMaxInflight ?? 8;
    if (activeCount >= cap) return;
    if (queueHasLlm !== "no") return;
    const candidates = queueRef.current.filter((item) => !item.status_guess);
    const toRun = candidates.filter((item) => !llmJobsRef.current[item.thread_id]);
    const slots = Math.max(0, cap - activeCount);
    if (!toRun.length || slots === 0) return;
    const batch = toRun.slice(0, slots).map((item) => item.thread_id);
    runJudgeBatch(batch, `Auto (${batch.length})`);
  }, [autoRun, bulkInfo?.running, llmJobs, llmMaxInflight, activeCount, queueHasLlm]);

  useEffect(() => {
    if (!autoRun) return;
    fetchQuotaState(true);
  }, [autoRun, llmModel, modelLabel]);

  return (
    <div className="min-h-screen bg-background text-foreground">
      <div className="grid grid-cols-[360px_1fr] gap-4 p-6">
        <Card className="h-[calc(100vh-48px)] overflow-hidden bg-card border-border flex flex-col sticky top-6">
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>Queue</CardTitle>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button size="sm" variant="secondary" className="h-7 px-2 text-xs">
                    <Play className="mr-1 h-3 w-3" />
                    Run LLM
                    {autoRun && (
                      <span className="ml-2 inline-flex items-center gap-1 text-[10px] text-emerald-600">
                        <span className="inline-flex h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-500" />
                        Auto
                      </span>
                    )}
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuItem
                    disabled={queue.length === 0}
                    onClick={() => runJudgeBatch(queue.map((q) => q.thread_id), `Visible (${queue.length})`)}
                  >
                    Run for visible ({queue.length})
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    disabled={queue.filter((q) => !q.status_guess).length === 0}
                    onClick={() =>
                      runJudgeBatch(
                        queue.filter((q) => !q.status_guess).map((q) => q.thread_id),
                        `Visible without LLM (${queue.filter((q) => !q.status_guess).length})`,
                      )
                    }
                  >
                    Run for visible without LLM ({queue.filter((q) => !q.status_guess).length})
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                    <DropdownMenuCheckboxItem
                      checked={autoRun}
                      onCheckedChange={(checked) => {
                        const next = Boolean(checked);
                        setAutoRun(next);
                        if (next) setAutoRunNote("");
                      }}
                    >
                    Auto-run (no LLM)
                  </DropdownMenuCheckboxItem>
                  <DropdownMenuSeparator />
                  <div className="px-2 py-1 text-[10px] uppercase text-muted-foreground">Model</div>
                  <DropdownMenuRadioGroup value={llmModel} onValueChange={setLlmModel}>
                    {MODEL_OPTIONS.map((model) => (
                      <DropdownMenuRadioItem key={model.value} value={model.value}>
                        {model.label}
                      </DropdownMenuRadioItem>
                    ))}
                  </DropdownMenuRadioGroup>
                  {bulkInfo?.running && (
                    <>
                      <DropdownMenuSeparator />
                      <DropdownMenuItem onClick={cancelBatch}>Stop scheduling</DropdownMenuItem>
                    </>
                  )}
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
            <div className="mt-3 space-y-2">
              {bulkInfo && (
                <div className="flex items-center justify-between text-[11px] text-muted-foreground">
                  <span>
                    Batch {bulkInfo.label}: {bulkInfo.queued}/{bulkInfo.total} queued
                  </span>
                </div>
              )}
              <div className="grid grid-cols-2 gap-2">
                <Select value={queueScope} onValueChange={setQueueScope}>
                  <SelectTrigger>
                    <SelectValue placeholder="Scope" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="unreviewed">Unreviewed</SelectItem>
                    <SelectItem value="reviewed">Reviewed</SelectItem>
                    <SelectItem value="all">All</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={queueHasLlm} onValueChange={setQueueHasLlm}>
                  <SelectTrigger>
                    <SelectValue placeholder="LLM" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="any">Any</SelectItem>
                    <SelectItem value="yes">Has LLM</SelectItem>
                    <SelectItem value="no">No LLM</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <Input
                placeholder="Search title"
                value={queueQuery}
                onChange={(e) => setQueueQuery(e.target.value)}
              />
              {queueQuery.trim() && (
                <div className="text-[11px] text-muted-foreground">
                  Searching within current filters.{" "}
                  <button
                    type="button"
                    className="text-foreground/80 underline-offset-2 hover:underline"
                    onClick={() => {
                      setQueueScope("all");
                      setQueueHasLlm("any");
                      setQueueStatusGuess("all");
                      setQueueConfidence("all");
                    }}
                  >
                    Clear filters
                  </button>
                </div>
              )}
              <div className="text-xs text-muted-foreground">
                Showing {queue.length} of {queueTotal}
              </div>
              {queueHasLlm === "yes" && (
                <>
                  <div className="text-xs text-muted-foreground">LLM filters</div>
                  <div className="grid grid-cols-2 gap-2">
                    <Select value={queueStatusGuess} onValueChange={setQueueStatusGuess}>
                      <SelectTrigger>
                        <SelectValue placeholder="Status guess" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All</SelectItem>
                        {STATUS_OPTIONS.map((s) => (
                          <SelectItem key={s} value={s}>
                            {s}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <Select value={queueConfidence} onValueChange={setQueueConfidence}>
                      <SelectTrigger>
                        <SelectValue placeholder="Confidence" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All</SelectItem>
                        {CONFIDENCE_OPTIONS.map((c) => (
                          <SelectItem key={c} value={c}>
                            {c}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </>
              )}
            </div>
          </CardHeader>
          <CardContent className="space-y-3 overflow-auto flex-1">
            {queue.map((item) => (
              <button
                key={item.thread_id}
                onClick={() => setSelectedId(item.thread_id)}
                className={`w-full text-left rounded-md border px-3 py-2 transition ${
                  item.thread_id === selectedId
                    ? "border-ring bg-muted"
                    : "border-border hover:border-muted-foreground"
                }`}
              >
                <div className="text-sm font-semibold line-clamp-3" title={item.title}>
                  {item.title}
                </div>
                <div className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
                  <span>#{item.thread_id}</span>
                  {item.decision_status ? (
                    <Badge variant="secondary" className="bg-muted text-foreground">
                      {item.decision_status}
                    </Badge>
                  ) : item.status_guess ? (
                    <Badge variant="secondary" className="bg-muted text-foreground" title="LLM guess (no decision yet)">
                      {item.status_guess}?
                      <span className="ml-1 text-[10px] uppercase text-muted-foreground/80">
                        {confidenceDots(item.confidence)}
                      </span>
                    </Badge>
                  ) : null}
                  {item.decision_status && item.status_guess && (
                    <span
                      className={`inline-flex flex-wrap items-center gap-x-1 gap-y-0.5 rounded-full border px-2 py-0.5 text-[10px] leading-none text-muted-foreground ${
                        item.status_guess === item.decision_status
                          ? "border-border/60 bg-muted/40"
                          : "border-muted-foreground/50 bg-muted/60"
                      }`}
                      title={`LLM guess ${item.status_guess} (${item.confidence ?? "?"}) ${
                        item.status_guess === item.decision_status ? "matches" : "disagrees with"
                      } decision`}
                    >
                      {item.status_guess === item.decision_status ? (
                        <CheckCircle2 className="h-3 w-3 opacity-60" />
                      ) : (
                        <XCircle className="h-3 w-3 text-purple-500/70" />
                      )}
                      <span className="uppercase whitespace-nowrap">LLM {confidenceDots(item.confidence)}</span>
                      <span className="whitespace-nowrap opacity-80">{item.status_guess}</span>
                    </span>
                  )}
                  {llmJobStatus(item.thread_id) === "running" && (
                    <span className="inline-flex items-center gap-1 rounded-full border border-blue-500/40 bg-blue-500/10 px-2 py-0.5 text-[10px] text-blue-600">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      LLM running
                    </span>
                  )}
                  {llmJobStatus(item.thread_id) === "queued" && (
                    <span className="inline-flex items-center gap-1 rounded-full border border-border/60 bg-muted/40 px-2 py-0.5 text-[10px] text-muted-foreground">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      LLM queued
                    </span>
                  )}
                  {llmJobStatus(item.thread_id) === "starting" && (
                    <span className="inline-flex items-center gap-1 rounded-full border border-border/60 bg-muted/40 px-2 py-0.5 text-[10px] text-muted-foreground">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      LLM starting
                    </span>
                  )}
                </div>
              </button>
            ))}
            {queue.length < queueTotal && (
              <Button variant="secondary" className="w-full" onClick={() => loadQueue(false)}>
                Load more
              </Button>
            )}
          </CardContent>
        </Card>

        <div className="space-y-4">
          <Card className="bg-card border-border h-[calc(100vh-48px)] flex flex-col">
            <CardHeader className="border-b border-border">
              <div className="flex flex-wrap items-start justify-between gap-2">
                <CardTitle className="leading-tight break-words" title={detail?.thread.title || ""}>
                  {detail?.thread.title || "Loading"} {detail?.thread.thread_id && `#${detail.thread.thread_id}`}
                </CardTitle>
                {detail?.thread?.url && (
                  <a
                    href={detail.thread.url}
                    target="_blank"
                    rel="noreferrer"
                    className="text-xs text-muted-foreground hover:text-foreground"
                  >
                    Open forum
                  </a>
                )}
              </div>
            </CardHeader>
            <CardContent className="flex-1 overflow-auto grid grid-cols-[1fr_360px] gap-4 pt-4">
              <div className={`space-y-3 ${loading ? "opacity-50 transition-opacity" : ""}`}>
                {detail?.posts.length ? (
                  detail?.posts.map((p) => (
                    <div
                      key={p.post_id}
                      className={`rounded-xl border border-border p-3 ${loading ? "pointer-events-none" : ""}`}
                    >
                      <div className="text-xs text-muted-foreground">
                        {p.post_id} · {p.author} · {p.posted_at}
                      </div>
                      <div className="mt-2 text-sm whitespace-pre-wrap">{p.body_text}</div>
                    </div>
                  ))
                ) : (
                  <div className="rounded-xl border border-dashed border-border p-4 text-sm text-muted-foreground">
                    No posts fetched for this thread.
                  </div>
                )}
              </div>

              <div className="space-y-3 sticky top-0 self-start">
                <Card className="bg-card border-border">
                  <CardHeader className="flex flex-wrap items-center gap-2">
                    <div className="flex items-baseline gap-2 leading-none shrink-0">
                      <CardTitle className="text-lg">LLM Summary</CardTitle>
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <button type="button" aria-label="LLM run behavior" className="inline-flex items-center">
                              <Info className="h-4 w-4 translate-y-[3px] text-muted-foreground" />
                            </button>
                          </TooltipTrigger>
                          <TooltipContent className="max-w-[260px]">
                            Runs per thread; you can navigate or save while it runs. Multiple runs can happen in parallel.
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                    </div>
                    <div className="flex flex-wrap items-center gap-2">
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Button
                              size="icon"
                              variant="secondary"
                              onClick={() => runJudge(selectedId)}
                              disabled={selectedId ? isLlmActive(selectedId) : false}
                              aria-label={detail?.judgment ? "Run LLM again" : "Run LLM"}
                              className="h-8 w-8"
                            >
                              {selectedId && isLlmActive(selectedId) ? (
                                <Loader2 className="h-4 w-4 animate-spin" />
                              ) : detail?.judgment ? (
                                <RotateCw className="h-4 w-4" />
                              ) : (
                                <Play className="h-4 w-4" />
                              )}
                            </Button>
                          </TooltipTrigger>
                          <TooltipContent>
                            {detail?.judgment ? "Run again" : "Run LLM"} · {modelLabel}
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button size="sm" variant="ghost" className="h-8 px-2 text-xs text-muted-foreground">
                            Next run: {modelLabel}
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                          <DropdownMenuRadioGroup value={llmModel} onValueChange={setLlmModel}>
                            {MODEL_OPTIONS.map((model) => (
                              <DropdownMenuRadioItem key={model.value} value={model.value}>
                                {model.label}
                              </DropdownMenuRadioItem>
                            ))}
                          </DropdownMenuRadioGroup>
                        </DropdownMenuContent>
                      </DropdownMenu>
                      {selectedId && isLlmActive(selectedId) && (
                        <TooltipProvider>
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <Button
                                size="icon"
                                variant="ghost"
                                onClick={() => cancelJudge(selectedId)}
                                aria-label="Cancel LLM run"
                                className="h-8 w-8"
                              >
                                <X className="h-4 w-4" />
                              </Button>
                            </TooltipTrigger>
                            <TooltipContent>Cancel</TooltipContent>
                          </Tooltip>
                        </TooltipProvider>
                      )}
                    </div>
                  </CardHeader>
                  <CardContent className="text-sm text-foreground/80 space-y-2">
                    {detail?.judgment?.summary ? (
                      <>
                        <div>{detail.judgment.summary}</div>
                        {detail.judgment.model && (
                          <div className="text-[11px] text-muted-foreground">
                            Model used: {detail.judgment.model}
                          </div>
                        )}
                        {detail.judgment.status_guess && detail.judgment.confidence && (
                          <div className="flex items-center gap-2 text-xs text-muted-foreground">
                            <Badge
                              variant="secondary"
                              className={
                                status !== detail.judgment.status_guess
                                  ? "border border-purple-500/30 bg-purple-500/10 text-purple-700"
                                  : "bg-muted text-foreground"
                              }
                            >
                              {detail.judgment.status_guess} · {detail.judgment.confidence}
                            </Badge>
                            {status !== detail.judgment.status_guess && (
                              <Button
                                size="sm"
                                variant="secondary"
                                className="h-5 gap-1 px-2 text-[10px] text-foreground"
                                onClick={applySuggestion}
                              >
                                Use <kbd className="rounded border border-border/60 bg-muted/60 px-1 py-0.5 text-[9px]">⌘ U</kbd>
                              </Button>
                            )}
                          </div>
                        )}
                      </>
                    ) : (
                      <div className="text-sm text-muted-foreground">
                        No LLM judgment for this thread yet.
                      </div>
                    )}
                  </CardContent>
                </Card>

                <Card className="bg-card border-border">
                  <CardHeader>
                    <div className="flex flex-wrap items-center gap-2">
                      <CardTitle className="text-base">Decision</CardTitle>
                      {detail?.decision?.status ? (
                        <span className="rounded-full border border-border/60 bg-muted/40 px-2 py-0.5 text-[10px] text-muted-foreground">
                          Saved: {STATUS_LABELS[detail.decision.status as (typeof STATUS_OPTIONS)[number]]}
                        </span>
                      ) : (
                        <span className="rounded-full border border-border/60 bg-muted/40 px-2 py-0.5 text-[10px] text-muted-foreground">
                          Not saved
                        </span>
                      )}
                      {detail?.decision?.status && status !== detail.decision.status && (
                        <span className="rounded-full border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 text-[10px] text-amber-700">
                          Unsaved changes
                        </span>
                      )}
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="text-xs text-muted-foreground">Pick a status</div>
                    <div className="grid grid-cols-3 gap-2">
                      {STATUS_OPTIONS.map((s) => (
                        <Button
                          key={s}
                          variant={status === s ? "default" : "secondary"}
                          onClick={() => setStatus(s)}
                          className="h-auto px-2 py-1 text-xs leading-tight whitespace-normal"
                        >
                          {STATUS_LABELS[s]}
                        </Button>
                      ))}
                    </div>

                    <div className="flex gap-2">
                      <Input
                        placeholder="Duplicate of (thread id)"
                        value={duplicateOf}
                        onChange={(e) => setDuplicateOf(e.target.value)}
                      />
                      <Dialog>
                        <DialogTrigger asChild>
                          <Button variant="secondary">Find</Button>
                        </DialogTrigger>
                        <DialogContent className="bg-card border-border">
                          <DialogHeader>
                            <DialogTitle>Find duplicate</DialogTitle>
                          </DialogHeader>
                          <Input
                            placeholder="Search title"
                            value={searchQuery}
                            onChange={(e) => setSearchQuery(e.target.value)}
                          />
                          <div className="mt-3 space-y-2 max-h-64 overflow-auto">
                            {searchResults.map((r) => (
                              <button
                                key={r.thread_id}
                                className="w-full text-left rounded-md border border-border px-3 py-2 hover:border-muted-foreground"
                                onClick={() => {
                                  setDuplicateOf(r.thread_id);
                                  setSearchQuery("");
                                  setSearchResults([]);
                                }}
                              >
                                <div className="text-sm font-medium">{r.title}</div>
                                <div className="text-xs text-muted-foreground">#{r.thread_id}</div>
                              </button>
                            ))}
                          </div>
                        </DialogContent>
                      </Dialog>
                    </div>

                    <div className="text-xs text-muted-foreground">Notes (optional)</div>
                    <Textarea
                      placeholder="Why this status? Duplicate reasoning?"
                      value={notes}
                      onChange={(e) => setNotes(e.target.value)}
                    />

                    <div className="flex items-center gap-2">
                      <Button onClick={saveDecision}>Save</Button>
                      <Button variant="secondary" onClick={saveAndNext}>
                        Save + Next
                      </Button>
                      {saveMsg && (
                        <span className="text-xs text-muted-foreground">{saveMsg}</span>
                      )}
                    </div>
                  </CardContent>
                </Card>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
      {jobEntries.length > 0 && (
        <div className="fixed bottom-4 right-4 w-80 rounded-md border border-border bg-card p-3 shadow-lg">
          <div className="flex items-center justify-between">
            <div className="text-sm font-semibold">LLM Runs</div>
            <div className="flex items-center gap-1">
              {activeCount > 0 && (
                <Button size="sm" variant="ghost" className="h-6 px-2 text-xs" onClick={cancelAllRuns}>
                  Cancel all
                </Button>
              )}
              {completedCount > 0 && (
                <Button size="sm" variant="ghost" className="h-6 px-2 text-xs" onClick={clearCompleted}>
                  Clear finished
                </Button>
              )}
            </div>
          </div>
          {autoRunNote && (
            <div className="mt-1 inline-flex items-center gap-2 rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 text-[11px] text-amber-700">
              {autoRunNote}
            </div>
          )}
          <div className="mt-2 h-1 w-full rounded bg-muted">
            <div className="h-1 rounded bg-foreground/60" style={{ width: `${progress}%` }} />
          </div>
          <div className="mt-2 text-xs text-muted-foreground">
            {jobCounts.queued || 0} queued · {jobCounts.starting || 0} starting · {jobCounts.running || 0} running ·{" "}
            {jobCounts.done || 0} done · {jobCounts.error || 0} failed · {jobCounts.cancelled || 0} cancelled ·{" "}
            {jobCounts.skipped || 0} skipped
          </div>
          {llmCapacityHint && (
            <div className="mt-1 text-[11px] text-muted-foreground">
              Queued due to concurrency cap{llmMaxInflight ? ` (max ${llmMaxInflight} in-flight)` : ""}.
            </div>
          )}
          <div className="mt-2 max-h-48 space-y-1 overflow-auto text-xs">
            {jobEntries.map(([threadId, job]) => (
              <button
                key={threadId}
                type="button"
                onClick={() => setSelectedId(threadId)}
                className="flex w-full items-center justify-between rounded border border-border/60 px-2 py-1 text-left transition hover:border-muted-foreground"
                title={job.status === "error" && job.error ? job.error : undefined}
              >
                <span>#{threadId}</span>
                <span
                  className={
                    job.status === "running"
                      ? "text-blue-500"
                      : job.status === "starting"
                      ? "text-amber-500"
                      : job.status === "queued"
                      ? "text-amber-500"
                      : job.status === "done"
                      ? "text-emerald-500"
                      : job.status === "error"
                      ? "text-red-500"
                      : job.status === "skipped"
                      ? "text-muted-foreground"
                      : "text-muted-foreground"
                  }
                >
                  {job.status}
                </span>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
