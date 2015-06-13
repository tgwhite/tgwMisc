
# Shamelessly borrowed function
# http://stackoverflow.com/questions/8425409/file-path-issues-in-r-using-windows-hex-digits-in-character-string-error
path_prep <- function(path = "clipboard") {
  y <- if (path == "clipboard") {
    readClipboard()
  } else {
    cat("Please enter the path:\n\n")
    readline()
  }
  x <- chartr("\\", "/", y)
  writeClipboard(x)
  return(x)
}
