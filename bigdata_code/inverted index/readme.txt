input text files with line format as:
line number <tab> line text

In driver code, specify InputFormat class as:
job.setInputFormatClass(KeyValueTextInputFormat.class);

Thus the line number at the beginning of each line will be parsed as key