// Make the "Guided tutorials" sidebar heading a link to the tutorials landing
// page, which is left out of the sidebar on purpose. If a theme update changes
// the heading markup, nothing matches and the heading stays plain text.
document.addEventListener("DOMContentLoaded", () => {
  const root = document.documentElement.dataset.content_root;
  if (root === undefined) return;
  const href = new URL("tutorials/index.html", new URL(root, document.baseURI)).href;
  for (const caption of document.querySelectorAll(".bd-sidebar-primary .caption-text")) {
    if (caption.textContent.trim() !== "Guided tutorials") continue;
    const link = document.createElement("a");
    link.href = href;
    link.style.color = "inherit";
    link.style.textDecoration = "none";
    link.textContent = caption.textContent;
    caption.replaceChildren(link);
  }
});
