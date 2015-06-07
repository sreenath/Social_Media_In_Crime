/*
 * HARVEST USING SEARCH API
 *   Program for collecting tweets related to crime using Search API
 *   Arguments Expected : Keywords related to crime like murder, rape, etc
 *   Collects recent and popular tweets with each keyword from up to a week
 */

import java.util.List;

import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Document;
import com.fourspaces.couchdb.Session;

public class SearchForTweets {
	public static void main(String[] args) {
		if (args.length < 1) {
			System.out
					.println("Enter crime related keywords as arguments");
			System.exit(-1);
		}

		Twitter twitter = new TwitterFactory().getInstance();

		Session dbSession = new Session("localhost", 5984);
		Database db = dbSession.getDatabase("crime_tweets");

		double dLatitude = -37.98397848013053;
		double dLongitude = 145.41961354921875;
		double dRadius = 34.8; // in Kilometers

		for (int i = 0; i < args.length; i++) {
			try {
				Query query = new Query(args[i]);
				query.geoCode(new GeoLocation(dLatitude, dLongitude), dRadius,
						Query.KILOMETERS.toString());
				QueryResult result;
				do {
					result = twitter.search(query);
					List<Status> tweets = result.getTweets();

					for (Status tweet : tweets) {
						String id = String.valueOf(tweet.getId());
						if (!tweet.isRetweet()) {
							Document doc = new Document();
							doc.setId(id);
							doc.put("id", id);
							doc.put("UserId", tweet.getUser().getId());
							doc.put("user", tweet.getUser().getScreenName());
							doc.put("text", tweet.getText());
							doc.put("created_at", tweet.getCreatedAt());
							doc.put("location", tweet.getGeoLocation());
							doc.put("lang", tweet.getLang());
							doc.put("place", tweet.getPlace());
							System.out.println(doc.toString());
							try {
								db.saveDocument(doc);
							} catch (Exception ex) {
								System.out.println("Error saving document - "
										+ ex.getMessage());
								continue;
							}
						}
					}
				} while ((query = result.nextQuery()) != null);
			} catch (TwitterException te) {
				te.printStackTrace();
				System.out.println("Failed to search tweets: "
						+ te.getMessage());
				try {
					System.out.println("Waiting..");
					Thread.sleep(1000 * 60 * 10);// wait for 10 minutes due to
													// rate limiting
				} catch (Exception ex) {
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.getMessage());
			}
		}
	}
}
