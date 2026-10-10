"use client";

import * as React from "react";
import { OctagonX, Play, ShieldOff, Square } from "lucide-react";

import { useServices } from "@/components/terminal/services";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { ApiError } from "@/lib/api/client";
import type { CommandKind } from "@/lib/api/types";
import { cn } from "@/lib/utils";

const COMMANDS: Record<
  CommandKind,
  { title: string; action: string; description: string; variant: "destructive" | "warning" | "buy" | "secondary" }
> = {
  halt: {
    title: "Halt the session",
    action: "Halt",
    description:
      "Kill switch: working orders are cancelled and no new risk is taken. Positions stay open. Resume lifts it.",
    variant: "warning",
  },
  flatten: {
    title: "Flatten every position",
    action: "Flatten",
    description:
      "Kill switch plus closing every position with reduce-only market orders. Use it to get out now.",
    variant: "destructive",
  },
  resume: {
    title: "Resume trading",
    action: "Resume",
    description:
      "Lifts the kill switch, the drawdown stop, the daily loss limit and reconciliation halts, and rebases the stop references to the current equity.",
    variant: "buy",
  },
  stop: {
    title: "Stop the engine",
    action: "Stop",
    description:
      "Stops the engine process gracefully: working orders are cancelled first. The session can be started again with `cq trade run`.",
    variant: "secondary",
  },
};

/** Operator commands for the selected session, each confirmed with a reason for the audit trail. */
export function CommandBar({ sessionId, running }: { sessionId: string; running: boolean }) {
  const [open, setOpen] = React.useState<CommandKind | null>(null);
  return (
    <div className="flex items-center gap-1">
      <Button size="xs" variant="warning" onClick={() => setOpen("halt")} title="Kill switch">
        <ShieldOff /> Halt
      </Button>
      <Button size="xs" variant="destructive" onClick={() => setOpen("flatten")}>
        <OctagonX /> Flatten
      </Button>
      <Button size="xs" variant="outline" onClick={() => setOpen("resume")}>
        <Play /> Resume
      </Button>
      <Button size="xs" variant="ghost" onClick={() => setOpen("stop")} disabled={!running}>
        <Square /> Stop
      </Button>
      {open !== null && (
        <CommandDialog sessionId={sessionId} kind={open} onClose={() => setOpen(null)} />
      )}
    </div>
  );
}

function CommandDialog({
  sessionId,
  kind,
  onClose,
}: {
  sessionId: string;
  kind: CommandKind;
  onClose(): void;
}) {
  const { api } = useServices();
  const spec = COMMANDS[kind];
  const [reason, setReason] = React.useState("");
  const [busy, setBusy] = React.useState(false);
  const [result, setResult] = React.useState<{ ok: boolean; message: string } | null>(null);

  const submit = async (event: React.FormEvent): Promise<void> => {
    event.preventDefault();
    if (!reason.trim() || busy) return;
    setBusy(true);
    setResult(null);
    try {
      const receipt = await api.command(sessionId, kind, reason.trim());
      setResult({
        ok: true,
        message: receipt.engine
          ? `Queued (offset ${receipt.offset}); engine ${receipt.engine} applies it now.`
          : `Queued (offset ${receipt.offset}); no engine is running — it applies when the session next starts.`,
      });
    } catch (error) {
      setResult({ ok: false, message: error instanceof ApiError ? error.message : String(error) });
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog open onOpenChange={(next) => (next ? undefined : onClose())}>
      <DialogContent>
        <form onSubmit={(event) => void submit(event)} className="flex flex-col gap-4">
          <DialogHeader>
            <DialogTitle>{spec.title}</DialogTitle>
            <DialogDescription>{spec.description}</DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-2">
            <Label htmlFor="command-reason">Reason (recorded in the audit trail)</Label>
            <Textarea
              id="command-reason"
              autoFocus
              maxLength={500}
              value={reason}
              onChange={(event) => setReason(event.target.value)}
              placeholder="e.g. exchange incident, end of experiment"
              disabled={busy || result?.ok === true}
            />
          </div>
          {result && (
            <p className={cn("text-sm", result.ok ? "text-buy" : "text-sell")} role="status">
              {result.message}
            </p>
          )}
          <DialogFooter>
            <Button type="button" variant="ghost" onClick={onClose}>
              {result?.ok ? "Close" : "Cancel"}
            </Button>
            {!result?.ok && (
              <Button type="submit" variant={spec.variant} disabled={!reason.trim() || busy}>
                {busy ? "Sending…" : `${spec.action} ${sessionId}`}
              </Button>
            )}
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
