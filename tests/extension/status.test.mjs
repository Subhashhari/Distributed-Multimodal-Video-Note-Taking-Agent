// Run: node --test tests/extension
import assert from "node:assert/strict";
import test from "node:test";

import {
  STEPS, errorText, failedStepIndex, mmss, retryText, statusLine, stepIndex, warningLines,
} from "../../extension/lib/status.js";

test("stepper labels match the spec exactly", () => {
  assert.deepEqual(STEPS.map((s) => s.label),
    ["Queued", "Transcribing", "Summarizing", "Extracting Frames", "Publishing", "Done"]);
});

test("step index per status", () => {
  assert.equal(stepIndex("QUEUED"), 0);
  assert.equal(stepIndex("EXTRACTING_FRAMES"), 3);
  assert.equal(stepIndex("COMPLETED"), 5);
  assert.equal(stepIndex("COMPLETED_WITH_WARNINGS"), 5);
  assert.equal(failedStepIndex({ error: { stage: "frames" } }), 3);
});

test("errors are human readable, never JSON", () => {
  assert.match(errorText({ code: "VIDEO_TOO_LONG" }), /longer than 20 minutes/);
  assert.match(errorText({ code: "VIDEO_UNAVAILABLE" }), /private/);
  assert.equal(errorText({ code: "SOMETHING_NEW", message: "Server said hi" }), "Server said hi");
  assert.ok(!errorText(null).includes("{"));
});

test("rate-limit waiting text", () => {
  const now = Date.parse("2026-11-06T10:00:00Z");
  const job = { retry: { waiting: true, reason: "RATE_LIMITED", api: "llm", attempt: 2, max_attempts: 6,
                         next_retry_at: "2026-11-06T10:00:27Z" } };
  assert.equal(retryText(job, now), "The AI service is busy, retrying in 27s (attempt 2 of 6)");
  assert.equal(retryText({ retry: null }), null);
});

test("warnings summary for omitted frames", () => {
  const job = { status: "COMPLETED_WITH_WARNINGS", result: { frames_requested: 6 },
                warnings: [{ code: "FRAME_EXTRACTION_FAILED", timestamp_sec: 461 }] };
  assert.equal(statusLine(job), "Done, with warnings");
  assert.deepEqual(warningLines(job),
    ["1 of 6 frames couldn't be added (07:41 couldn't be captured). The page was published without them."]);
});

test("mmss", () => {
  assert.equal(mmss(0), "00:00");
  assert.equal(mmss(1147), "19:07");
  assert.equal(mmss(3725), "1:02:05");
});

test("dead-lettered line mentions attempts", () => {
  assert.equal(statusLine({ status: "DEAD_LETTERED", error: { attempts: 5 } }),
    "Couldn't process after 5 attempts");
});
