"use client";

import * as React from "react";
import { Loader2 } from "lucide-react";

import { TerminalServices } from "@/components/terminal/services";
import { SignIn } from "@/components/terminal/sign-in";
import { Terminal } from "@/components/terminal/terminal";
import { TooltipProvider } from "@/components/ui/tooltip";
import { useAuthStore } from "@/stores/auth";
import { useSelectionStore } from "@/stores/selection";

/**
 * The client application. The token and the remembered selection live in browser storage, so
 * nothing is rendered until they are read (the server-rendered page is a neutral splash, which
 * also keeps hydration free of mismatches).
 */
export function TerminalApp() {
  const authReady = useAuthStore((state) => state.hydrated);
  const token = useAuthStore((state) => state.token);
  const selectionReady = useSelectionStore((state) => state.hydrated);

  React.useEffect(() => {
    useAuthStore.getState().hydrate();
    useSelectionStore.getState().hydrate();
  }, []);

  if (!authReady || !selectionReady) return <Splash />;
  return (
    <TooltipProvider delayDuration={250}>
      {token === null ? (
        <SignIn />
      ) : (
        <TerminalServices key={token} token={token}>
          <Terminal />
        </TerminalServices>
      )}
    </TooltipProvider>
  );
}

function Splash() {
  return (
    <main className="flex min-h-dvh items-center justify-center bg-background text-muted-foreground">
      <Loader2 className="size-5 animate-spin" aria-label="Loading" />
    </main>
  );
}
