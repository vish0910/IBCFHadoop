/**
 * 
 */

/**
 * @author Vishal Doshi
 *
 */

public class MovieRating implements Comparable<MovieRating> {
	Long movieId;
	int movieRating;
	
	public MovieRating(Long movieId, int movieRating) {
		this.movieId = movieId;
		this.movieRating = movieRating;
	}

	public MovieRating(String s) {
		String s1 = s.substring(1);
		String[] vals = s1.split(",");
		this.movieId = Long.parseLong(vals[0]);
		this.movieRating = Integer.parseInt(vals[1]);
	}
	
	@Override
	public int compareTo(MovieRating o) {
		return this.movieId>o.movieId?1:this.movieId<o.movieId?-1:0;
	}

	@Override
	public String toString() {
		return "(" + movieId + ","
				+ movieRating + ")";
	}

}
