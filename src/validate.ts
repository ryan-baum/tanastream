// F1 — Tana-Paste injection guard (reject-loud, opt-in raw).
//
// The Local API create path is Tana-Paste-only (no structured literal-create endpoint), so
// user content containing Tana Paste control syntax gets reinterpreted: `::` -> field,
// `#tag` -> supertag, leading `- ` -> nested bullet, `[[ ]]` -> reference, `^id` -> ref id,
// `%%...%%` -> directive, `N. ` -> ordered list. We reject such content LOUDLY at enqueue
// rather than escape it (escaping Tana Paste safely is not actually possible without mutating
// bytes). Producers that intentionally want Tana syntax opt in via `rawTanaPaste`/`tanaPaste`.

export interface CreateContent {
  name?: unknown;
  description?: unknown;
  children?: unknown;
  tanaPaste?: unknown;
  rawTanaPaste?: unknown;
}

interface HazardRule {
  label: string;
  test: (text: string) => boolean;
}

const HAZARD_RULES: HazardRule[] = [
  { label: "'::' would become a Tana field", test: (t) => t.includes("::") },
  { label: "'%%' is a Tana Paste directive sigil", test: (t) => t.includes("%%") },
  { label: "'[[' would become a node reference", test: (t) => t.includes("[[") },
  { label: "'^' would become a node-id reference", test: (t) => t.includes("^") },
  { label: "leading/whitespace '#' would become a supertag", test: (t) => /(^|\s)#/.test(t) },
  { label: "leading '- ' would become a nested bullet", test: (t) => /^-\s/.test(t.trim()) },
  { label: "leading 'N. ' would become an ordered-list item", test: (t) => /^\d+\.\s/.test(t.trim()) },
];

/** Returns a human-readable hazard description for the first Tana-Paste control sequence found, or null if literal-safe. */
export function findTanaHazard(text: string): string | null {
  for (const rule of HAZARD_RULES) {
    if (rule.test(text)) return rule.label;
  }
  return null;
}

function isRaw(payload: CreateContent): boolean {
  return typeof payload.tanaPaste === "string" || payload.rawTanaPaste === true;
}

/**
 * Throws (fail-closed) if a structured create's name/description/children contain Tana Paste
 * control syntax. No-op when the producer opted into raw Tana Paste. Call only for create ops.
 */
export function assertCreateSafe(payload: CreateContent): void {
  if (isRaw(payload)) return;

  const fields: Array<{ where: string; value: string }> = [];
  if (typeof payload.name === "string") fields.push({ where: "name", value: payload.name });
  if (typeof payload.description === "string") fields.push({ where: "description", value: payload.description });
  if (Array.isArray(payload.children)) {
    payload.children.forEach((child, i) => {
      if (typeof child === "string") fields.push({ where: `child[${i}]`, value: child });
      else if (child && typeof child === "object" && "name" in child) {
        // buildTanaPaste stringifies ANY object child name (String(child.name)); mirror that here so a
        // non-string (e.g. array-wrapped) hazard cannot bypass the denylist through the typed hole.
        fields.push({ where: `child[${i}].name`, value: String((child as { name: unknown }).name) });
      }
    });
  }

  for (const { where, value } of fields) {
    const hazard = findTanaHazard(value);
    if (hazard) {
      throw new Error(
        `TANA_PASTE_UNSAFE: create ${where} ${JSON.stringify(value)} contains control syntax (${hazard}). ` +
          `It would be silently misparsed on the Local API. Rephrase, or set rawTanaPaste:true / use --payload-json to opt into raw Tana syntax intentionally.`,
      );
    }
  }
}

/**
 * Throws (fail-closed) if an explicit producer idempotencyKey is not collapse-stable — i.e. it has
 * leading/trailing/repeated/control whitespace that Tana collapses or trims in the marker read-back.
 * The on-node marker is `PREFIX + dedupKey`; if the key is not collapse-stable, the written marker
 * never equals the collapsed read-back, verification fails on every attempt, and the create
 * orphan-duplicates up to maxAttempts (the BUG-2 amplification class at the key seam). Auto-hash keys
 * are hex and always pass; only explicit producer keys need this check. Call for create ops only.
 */
export function assertKeySafe(idempotencyKey: string): void {
  const collapsed = idempotencyKey.replace(/\s+/g, " ").trim();
  if (idempotencyKey !== collapsed) {
    throw new Error(
      `TANA_KEY_UNSAFE: idempotencyKey ${JSON.stringify(idempotencyKey)} has leading/trailing/repeated/control ` +
        `whitespace that Tana collapses in the idempotency-marker read-back — it would break marker matching and ` +
        `orphan-duplicate the create. Use a collapse-stable key (no tabs/newlines/double-spaces/edge spaces).`,
    );
  }
}
