r-main: 
	Rscript -e "rmarkdown::render('carney_condo_report.Rmd', output_format = 'pdf_document')"

julia-main: 
	julia --project=. src/weave_doc.jl 