"use client";

import * as React from "react";
import { AlertTriangle, RotateCcw } from "lucide-react";

import { Button } from "@/components/ui/button";

interface PanelBoundaryProps {
  /** Shown in the fallback: "The {name} panel failed". */
  name: string;
  /** The boundary resets itself when this changes (a new market, session or timeframe). */
  resetKey?: string;
  children: React.ReactNode;
}

interface PanelBoundaryState {
  error: Error | null;
}

/**
 * Contains a rendering failure to one panel: a chart that throws on unexpected data shows a
 * message and a retry button, while the rest of the terminal (and the kill switches) keep
 * working.
 */
export class PanelBoundary extends React.Component<PanelBoundaryProps, PanelBoundaryState> {
  override state: PanelBoundaryState = { error: null };

  static getDerivedStateFromError(error: unknown): PanelBoundaryState {
    return { error: error instanceof Error ? error : new Error(String(error)) };
  }

  override componentDidCatch(error: Error, info: React.ErrorInfo): void {
    console.error(`${this.props.name} panel failed`, error, info.componentStack);
  }

  override componentDidUpdate(previous: PanelBoundaryProps): void {
    if (this.state.error !== null && previous.resetKey !== this.props.resetKey) {
      this.setState({ error: null });
    }
  }

  private readonly retry = (): void => {
    this.setState({ error: null });
  };

  override render(): React.ReactNode {
    const { error } = this.state;
    if (error === null) return this.props.children;
    return (
      <div
        role="alert"
        className="flex min-h-0 flex-1 flex-col items-center justify-center gap-2 rounded-md border border-sell/40 bg-card px-6 text-center text-xs"
      >
        <AlertTriangle className="size-5 text-sell" />
        <p className="font-medium text-foreground">The {this.props.name} panel failed</p>
        <p className="max-w-md break-words text-muted-foreground">{error.message}</p>
        <Button size="xs" variant="outline" onClick={this.retry}>
          <RotateCcw /> Retry
        </Button>
      </div>
    );
  }
}
