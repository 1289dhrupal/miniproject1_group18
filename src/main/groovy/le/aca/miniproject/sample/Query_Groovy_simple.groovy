package le.aca.miniproject.sample

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

// from JSON file to Groovy map
def jsonSlurper = new JsonSlurper()
def file = new File('src/main/resources/top-rated-movies.json')
//def file = new File('src/main/resources/userdetails.json')
def movieList = jsonSlurper.parseText(file.text)


/**
 * Example of simple query:
 * 
 * Select movies with more than a 9 rating
 * and project title and year.
 * Then the resulting movies are sorted by year, in ascending order. 
 */
def projection = {
	[
		title: it.title,
		year: it.year
	]
}

def selectedMovies2 = movieList
	.findAll{ it.imdbRating > 9}
	.collect{projection(it)}
	.sort { it.year }

println(JsonOutput.prettyPrint(JsonOutput.toJson(selectedMovies2)))


