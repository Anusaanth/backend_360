# Create a data frame with two columns and 5 rows
data <- data.frame(lic = rep(1, 5), uwi = rep(1, 5))

# Write the data frame to a CSV file
current_directory <- getwd()
setwd(current_directory)
output_file_path <- file.path(current_directory,'output_file.csv')
output_file_normalized_path <- normalizePath(output_file_path )
write.csv(data, output_file_normalized_path, row.names = FALSE)
