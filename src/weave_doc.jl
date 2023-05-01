using Weave


filename = normpath("src/summarize_condos_and_departments.jmd")
weave(filename, out_path = :pwd, doctype = "md2pdf")