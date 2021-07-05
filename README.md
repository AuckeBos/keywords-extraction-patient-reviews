# Keywords Extraction on patient reviews of ZorgkaartNederland
A tool to extract keywords from short (30-80 words) text reviews on ZorgkaartNederland

This repo is the result of an internship for the Master Data Science at Radboud University.
The structure of the repo is as follows:

- [Report.pdf](Report.pdf) contains the report of the internship. It elaborates upon the motivation, structure and
  results of the project. Please consult it for more information about the system.
- [scraper.R](R/scraper.R) provides functionality to scrape reviews of ZorgkaartNederland.
- [keyword_extraction.R](R/keyword_extraction.R) implements the algorithm as described in the [Report](Report.pdf).
- [sentiment_analysis.R](R/sentiment_analysis.R) and [geographical_analysis.R](R/geographical_analysis.R) provide the 
  functionality of the proof of concept, as described in the [Report](Report.pdf)
- [helpers.R](R/helpers.R) provides some helper functionality
