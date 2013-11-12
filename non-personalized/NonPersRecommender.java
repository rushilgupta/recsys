import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class NonPersRecommender {
	
	private Map<String,List<Integer>> _userData;
	private List<Integer> _movieList;

	private static final int THRESHOLD = 0;
	private static final int NUM_OUTPUT = 5;
	
	NonPersRecommender() {
		_userData = new HashMap<String, List<Integer>>();
		_movieList = new ArrayList<Integer>();
	}
	
	/** 
	 * calculate score for recommended movie wrt movieId
	 *  score = (x & y)/x
	 *  where x is # of users who rated movieId above threshold
	 *  & y is # of users who rated recommendedMovieId above threshold
	 */
	private float calculateScore( int movieId, int recommendedMovieId )	{
		int movieIdUserCount = getUserCount( movieId );
		int bothMovieUserCount = getUserCount( movieId, recommendedMovieId );
		return (float) bothMovieUserCount/movieIdUserCount;
	}
	
	/** 
	 * calculate advanced score for recommended movie wrt movieId
	 *  score = (x & y)/x / (!x & y)/!x
	 *  where x is # of users who rated movieId above threshold
	 *  & !x is # of users did not rate movieId above threshold
	 *  
	 *  advanced score is resistant to majority bias
	 */
	private float calculateAdvScore( int movieId, int recommendedMovieId )	{
		int movieIdUserCount = getUserCount( movieId );
		int bothMovieUserCount = getUserCount( movieId, recommendedMovieId );
		
		int notMovieIdUserCount = getNotUserCount( movieId );
		int notMovieIdRecommendeduserCount = getNotUserCount( movieId, recommendedMovieId );
		return  ( (float) bothMovieUserCount/movieIdUserCount ) / 
				 ( (float) notMovieIdRecommendeduserCount/notMovieIdUserCount );
	}
	
	/** return a list of top5 movieRatings tuple sorted by highest score */
	public List<MovieRatings> recommendedFor( int movieId )	{
		List<MovieRatings> movieRatings = new ArrayList<MovieRatings>();
		
		for( int recommendedMovieId : _movieList )	{
			//float score = calculateScore( movieId, recommendedMovieId );
			float score = calculateAdvScore( movieId, recommendedMovieId );
			movieRatings.add( new MovieRatings( recommendedMovieId, score ) );
		}
		
		Collections.sort( movieRatings );
		// max score is always for recommendedMovieId = movieId
		return movieRatings.subList( 1, NUM_OUTPUT + 1 );
	}
	
	/** get # of users who rated movie above threshold */
	private int getUserCount( int movie ) {
		int count = 0;
		for( Entry<String, List<Integer>> entry : _userData.entrySet() )	{
			if( entry.getValue().contains( movie ) )	{
				count++;
			}
		}
		return count;
	}
	
	/** get # of users who rated both movieA and movieB above threshold */
	private int getUserCount( int movieA, int movieB )	{
		int count = 0;
		for( Entry<String, List<Integer>> entry : _userData.entrySet() )	{
			if( entry.getValue().contains( movieA ) && 
					entry.getValue().contains( movieB ) )	{
				count++;
			}
		}
		return count;
	}
	
	/** get # of users who did not rate movie above threshold */
	private int getNotUserCount( int movie ) {
		int count = 0;
		for( Entry<String, List<Integer>> entry : _userData.entrySet() )	{
			if( !entry.getValue().contains( movie ) )	{
				count++;
			}
		}
		return count;
	}
	
	/** get # of users who did not rate movieA but rated movieB above threshold */
	private int getNotUserCount( int movieA, int movieB )	{
		int count = 0;
		for( Entry<String, List<Integer>> entry : _userData.entrySet() )	{
			if( !entry.getValue().contains( movieA ) && 
					entry.getValue().contains( movieB ) )	{
				count++;
			}
		}
		return count;
	}

	/** helper class to store <movieId,score> tuple */
	public static class MovieRatings implements Comparable<MovieRatings> {
		private int _movieId;
		private float _score;
		
		public MovieRatings( int movieId, float score ) {
			_movieId = movieId;
			_score = score;
		}
		
		public int getMovieId() {
			return _movieId;
		}
		
		public float getScore() {
			return _score;
		}

		@Override
		public int compareTo( MovieRatings mvr ) {
			if( this._score > mvr._score ) {
				return -1;
			}
			else if( this._score < mvr._score ) {
				return 1;
			}
			else {
				return 0;
			}
		}
	}
	
	/** add data to data structures */
	private void addUserData( String uid, Integer movieId ) {
		if( _userData.containsKey( uid ) ) {
			_userData.get(uid).add( movieId );
		}
		else {
			List<Integer> movieList = new ArrayList<Integer>();
			movieList.add( movieId );
			_userData.put( uid, movieList );
		}
		
		if( !_movieList.contains(movieId) )	{
			_movieList.add( movieId );
		}
	}
	
	/** parse ratings file and split data */
	public void parseFile( String filepath ) {
		String line = "";
		BufferedReader br = null;
		try
		{
			br = new BufferedReader( new FileReader( filepath ) );
			while ( ( line = br.readLine()) != null ) {
				String[] temp = line.split( "," );
				
				if( Integer.parseInt( temp[1] ) > THRESHOLD )	{
					addUserData( temp[0], Integer.parseInt( temp[1] ) );
				}
			}
		} catch ( IOException e ) {
			e.printStackTrace();
		} finally {
			try {
				if ( br != null ) {
					br.close();
				}
			} catch ( IOException ex ) {
				ex.printStackTrace();
			}
		}
	}
	
	public static void main( String[] args ) {
		// Movies array contains the movie IDs of the top 5 movies.
		int movies[] = {11, 121, 8587};
		//int movies[] = {122, 603, 194};
		
		NonPersRecommender rec = new NonPersRecommender();
        if( args == null || args.length != 1 || args[0] == null ) {
            System.err.println( "USAGE: NonPersRecommender <filePath>" );
            System.exit(1);
        }
		rec.parseFile( args[0] );
		
		for( int movieId : movies )	{
			List<MovieRatings> list = rec.recommendedFor( movieId );
			System.out.format( "%d", movieId );
			for( MovieRatings mvr : list ) {
				System.out.format( ",%d,%.2f", mvr.getMovieId(), mvr.getScore() );
			}
			System.out.format("%n");
		}
	}
}
