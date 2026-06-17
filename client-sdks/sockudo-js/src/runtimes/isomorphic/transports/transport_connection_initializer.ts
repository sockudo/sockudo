/** Initializes the transport.
 *
 * Fetches resources if needed and then transitions to initialized.
 */
export default function () {
  this.timeline.info(
    this.buildTimelineMessage({
      transport: this.name + (this.options.useTLS ? "s" : ""),
    }),
  );

  if (this.hooks.isInitialized()) {
    this.changeState("initialized");
  } else {
    this.onClose();
  }
}
