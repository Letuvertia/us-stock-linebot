function handlePodcastSummarized(payload: {
  id: string;
  title: string;
  summary: string;
  episode_url: string;
}): void {
  const { title, summary, episode_url } = payload;

  const timeLabel = Utilities.formatDate(new Date(), TIMEZONE, 'yyyy/M/d HH:mm');
  let msg = `🎙️ 股癌摘要 (${timeLabel})\n`;
  msg += `──────────────\n`;
  msg += `${title}\n\n`;
  msg += summary;
  if (episode_url) msg += `\n\n${episode_url}`;

  sendPushMessage(msg);
}
