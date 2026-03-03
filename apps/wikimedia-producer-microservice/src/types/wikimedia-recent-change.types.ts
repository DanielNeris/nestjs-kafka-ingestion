/**
 * Meta block from the stream (request/stream identifiers).
 * @see https://stream.wikimedia.org/v2/stream/recentchange
 */
export interface WikimediaRecentChangeMeta {
  uri?: string;
  request_id?: string;
  id?: string;
  domain?: string;
  stream?: string;
  dt?: string;
  topic?: string;
  partition?: number;
  offset?: number;
}

/**
 * Shape of SSE "data" from stream.wikimedia.org/v2/stream/recentchange.
 * Field names match the API (snake_case). Optional for type/edit/log variants.
 * @see https://www.mediawiki.org/wiki/API:Recent_changes_stream
 * @see https://stream.wikimedia.org/v2/stream/recentchange
 */
export interface WikimediaRecentChangePayload {
  $schema?: string;
  meta?: WikimediaRecentChangeMeta;
  id?: number;
  type?: string;
  namespace?: number;
  title?: string;
  title_url?: string;
  comment?: string;
  timestamp?: number;
  user?: string;
  bot?: boolean;
  wiki?: string;
  server_url?: string;
  server_name?: string;
  server_script_path?: string;
  notify_url?: string;
  parsedcomment?: string;
  minor?: boolean;
  patrolled?: boolean;
  length?: { old?: number; new?: number };
  revision?: { old?: number; new?: number };
  log_id?: number;
  log_type?: string;
  log_action?: string;
  log_params?: Record<string, unknown>;
  log_action_comment?: string;
}
