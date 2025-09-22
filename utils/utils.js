async function findStringBetween(str, startDelimiter, endDelimiter) {
  // Create a regular expression to capture the string between the delimiters
  const regex = new RegExp(`${startDelimiter}(.*?)${endDelimiter}`);

  // Use the regex to find the match
  const match = str.match(regex);

  // If a match is found, return the captured group (substring between the delimiters)
  if (match && match[1]) {
    return match[1].trim(); // Trimming to remove any leading/trailing spaces
  } else {
    return null; // Return null if no match is found
  }
}

module.exports = { findStringBetween };
