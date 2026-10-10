"use client";

import * as React from "react";
import { CheckCircle2, CircleAlert, KeyRound, Loader2, RefreshCw } from "lucide-react";

import { Logo } from "@/components/terminal/top-bar";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ApiClient, ApiError } from "@/lib/api/client";
import type { Health } from "@/lib/api/types";
import { apiBaseUrl } from "@/lib/config";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/stores/auth";

type GatewayState =
  | { kind: "checking" }
  | { kind: "up"; health: Health }
  | { kind: "down"; message: string };

/**
 * Sign-in with the gateway's operator token (`CQ_API_TOKEN`). The token is verified against the
 * gateway before it is stored, so a typo is caught here rather than as a failing stream.
 */
export function SignIn() {
  const baseUrl = React.useMemo(() => apiBaseUrl(), []);
  const notice = useAuthStore((state) => state.notice);
  const signIn = useAuthStore((state) => state.signIn);
  const [gateway, setGateway] = React.useState<GatewayState>({ kind: "checking" });
  const [token, setToken] = React.useState("");
  const [remember, setRemember] = React.useState(false);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const probe = React.useCallback(async (): Promise<void> => {
    setGateway({ kind: "checking" });
    try {
      const health = await new ApiClient({ baseUrl, token: null, timeoutMs: 5_000 }).health();
      setGateway({ kind: "up", health });
    } catch (reason) {
      setGateway({ kind: "down", message: reason instanceof Error ? reason.message : String(reason) });
    }
  }, [baseUrl]);

  React.useEffect(() => {
    void probe();
  }, [probe]);

  const submit = async (event: React.FormEvent): Promise<void> => {
    event.preventDefault();
    const candidate = token.trim();
    if (!candidate || busy) return;
    setBusy(true);
    setError(null);
    try {
      await new ApiClient({ baseUrl, token: candidate }).checkToken();
      signIn(candidate, remember);
    } catch (reason) {
      if (reason instanceof ApiError && reason.unauthorized) {
        setError("The gateway rejected this token.");
      } else {
        setError(reason instanceof Error ? reason.message : String(reason));
        void probe();
      }
      setBusy(false);
    }
  };

  return (
    <main className="flex min-h-dvh items-center justify-center bg-background p-4">
      <Card className="w-full max-w-md gap-5">
        <CardHeader>
          <div className="flex items-center gap-2.5">
            <Logo className="size-8" />
            <div>
              <CardTitle className="text-base">Crypto Quant Terminal</CardTitle>
              <CardDescription>Sign in with the gateway&apos;s operator token.</CardDescription>
            </div>
          </div>
        </CardHeader>
        <form onSubmit={(event) => void submit(event)}>
          <CardContent className="flex flex-col gap-4">
            {notice && (
              <p className="rounded-md border border-warning/40 bg-warning-muted px-3 py-2 text-xs text-warning" role="status">
                {notice}
              </p>
            )}
            <GatewayStatus baseUrl={baseUrl} state={gateway} onRetry={() => void probe()} />
            <div className="flex flex-col gap-2">
              <Label htmlFor="token">API token</Label>
              <Input
                id="token"
                type="password"
                autoComplete="current-password"
                autoFocus
                spellCheck={false}
                value={token}
                onChange={(event) => setToken(event.target.value)}
                placeholder="CQ_API_TOKEN"
                aria-invalid={error !== null}
                disabled={busy}
              />
              <p className="text-[11px] text-muted-foreground">
                The value of <code className="figure">CQ_API_TOKEN</code> in the gateway&apos;s
                environment (<code className="figure">make env</code> generates one).
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Checkbox
                id="remember"
                checked={remember}
                onCheckedChange={(checked) => setRemember(checked === true)}
                disabled={busy}
              />
              <Label htmlFor="remember" className="text-xs font-normal">
                Remember on this device (otherwise only for this tab)
              </Label>
            </div>
            {error && (
              <p className="text-xs text-sell" role="alert">
                {error}
              </p>
            )}
          </CardContent>
          <CardFooter className="mt-5">
            <Button type="submit" className="w-full" disabled={!token.trim() || busy}>
              {busy ? <Loader2 className="animate-spin" /> : <KeyRound />}
              {busy ? "Checking…" : "Sign in"}
            </Button>
          </CardFooter>
        </form>
      </Card>
    </main>
  );
}

function GatewayStatus({
  baseUrl,
  state,
  onRetry,
}: {
  baseUrl: string;
  state: GatewayState;
  onRetry(): void;
}) {
  const healthy = state.kind === "up" && state.health.status === "ok";
  return (
    <div
      className={cn(
        "flex items-start gap-2 rounded-md border px-3 py-2 text-xs",
        state.kind === "down" && "border-sell/40 text-sell",
        state.kind === "up" && !healthy && "border-warning/40 text-warning",
      )}
      data-testid="gateway-status"
    >
      {state.kind === "checking" ? (
        <Loader2 className="mt-px size-3.5 shrink-0 animate-spin text-muted-foreground" />
      ) : healthy ? (
        <CheckCircle2 className="mt-px size-3.5 shrink-0 text-buy" />
      ) : (
        <CircleAlert className="mt-px size-3.5 shrink-0" />
      )}
      <div className="flex min-w-0 flex-1 flex-col gap-0.5">
        <span className="figure truncate text-foreground">{baseUrl}</span>
        <span className={cn(state.kind !== "down" && "text-muted-foreground")}>
          {state.kind === "checking" && "Checking the gateway…"}
          {state.kind === "down" && state.message}
          {state.kind === "up" &&
            (healthy
              ? `Gateway ${state.health.version} is up.`
              : `Gateway ${state.health.version} is degraded: ${[
                  state.health.database ? null : "database unreachable",
                  state.health.redis ? null : "Redis unreachable",
                ]
                  .filter(Boolean)
                  .join(", ")}.`)}
        </span>
      </div>
      {state.kind !== "checking" && (
        <Button type="button" size="icon-sm" variant="ghost" onClick={onRetry} aria-label="Check again">
          <RefreshCw />
        </Button>
      )}
    </div>
  );
}
