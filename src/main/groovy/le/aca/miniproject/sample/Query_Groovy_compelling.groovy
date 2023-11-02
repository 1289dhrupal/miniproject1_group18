package le.aca.miniproject.sample

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

// from JSON file to Groovy map
def jsonSlurper = new JsonSlurper()
def file = new File('src/main/resources/top-rated-movies.json')
//def file = new File('src/main/resources/userdetails.json')
def movieList = jsonSlurper.parseText(file.text)

/**
 * Example of query that requires using Groovy in more depth.
 * 
 * Select actors that star more than 1 movie with at least a 9 imdb rating
 */

// projection extracting relevant fields
def projection = {
	[
		title: it.title,
		year: it.year,
		actors: it.actors,
		imdbRating : it.imdbRating
	]
}

// selects movies using filter and applies projection
def selectedMovies = movieList
	.findAll{ it.imdbRating >= 9}
	.collect{projection(it)}

// prepares a list of movies with one actor
// making a copy of the movie per actor
def moviesWithOneActor = []
for (movie in selectedMovies) {
	for (actor in movie.actors) {
		// clone is used to create a copy of the object
		// without clone() we would have one single objects
		// with different references to it
		def modifiedMovie=movie.clone()
		modifiedMovie.singleActor = actor
		println("added:" + modifiedMovie)
		moviesWithOneActor << modifiedMovie 
	}
}

// groups movies by actor in order to know how many movies have been starred by an actor
def groupedMovies = moviesWithOneActor.groupBy { it.singleActor }

// selects those entries where an actor stars more than one movie
def selectedGroupMovies = groupedMovies.findAll{it.value.size()>1}

// List of documents
println(JsonOutput.prettyPrint(JsonOutput.toJson(selectedGroupMovies)))

// Final report
for (entry in selectedGroupMovies) {
	println(entry.key)
	for (movie in entry.value) {
		println("	${movie.title} (${movie.year})")	
	} 
}


