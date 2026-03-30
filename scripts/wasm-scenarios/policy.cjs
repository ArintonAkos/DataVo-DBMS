const ATTR_IGNORE = "BrowserTranslateIgnore";
const ATTR_NEEDS_SPECIFIC = "BrowserTranslateNeedsSpecificCode";

function parseReasonFromAttribute(attributeText, attributeName) {
  const escapedName = attributeName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const attrRegex = new RegExp(
    `\\[\\s*${escapedName}(?:Attribute)?\\s*(?:\\(([^\\)]*)\\))?\\s*\\]`,
    "i",
  );
  const match = attributeText.match(attrRegex);
  if (!match) {
    return null;
  }

  const args = (match[1] || "").trim();
  if (!args) {
    return "";
  }

  const reasonMatch = args.match(/"((?:[^"\\]|\\.)*)"/);
  return reasonMatch ? reasonMatch[1] : args;
}

function isIntentionallyNonBrowserLane(sourcePath) {
  return (
    sourcePath.startsWith("DataVo.Tests/ADO/") ||
    sourcePath.startsWith("DataVo.Tests/EntityFramework/")
  );
}

module.exports = {
  ATTR_IGNORE,
  ATTR_NEEDS_SPECIFIC,
  parseReasonFromAttribute,
  isIntentionallyNonBrowserLane,
};
