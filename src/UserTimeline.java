/*
 * HARVEST TWEETS BY READING USER TIMELINE
 *   Sample program to collect tweets from important sources
 *   eg.: VictoriaPolice, CrimeStoppers, Newspapers, etc 
 *   Arguments Expected : Screen names of users to read timeline
 *   Reads up to 3200 tweets from same user
 */

import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Document;
import com.fourspaces.couchdb.Session;

public class UserTimeline {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out
					.println("Enter User screen names as arguments");
			System.exit(-1);
		}
		Session dbSession = new Session("146.118.97.20", 5984);

		if (!dbSession.getDatabaseNames().contains("crime_tweets")) {
			dbSession.createDatabase("crime_tweets");
		}
		final Database db = dbSession.getDatabase("crime_tweets");

		ResponseList<Status> statuses = null;
		Twitter twitter = new TwitterFactory().getInstance();

		for (int j = 0; j < args.length; j++) {
			for (int i = 1; i < 200; i++) {
				Paging paging = new Paging(i, 200);
				try {
					statuses = twitter.getUserTimeline(args[j], paging);
				} catch (TwitterException te) {
					try {
						System.out.println("Waiting..");
						Thread.sleep(1000 * 60 * 10);// wait for 10 minutes due
														// to
														// limits
					} catch (Exception ex) {
					}
				} catch (Exception ex) {
					System.out.println("Exception");
					continue;
				}
				for (Status status : statuses) {
					if (!status.isRetweet()) {
						Document doc = new Document();
						String id = String.valueOf(status.getId());
						doc.setId(id);
						doc.put("id", id);
						doc.put("UserId", status.getUser().getId());
						doc.put("user", status.getUser().getScreenName());
						doc.put("text", status.getText());
						doc.put("created_at", status.getCreatedAt());
						doc.put("location",
								String.valueOf(status.getGeoLocation()
										.getLatitude())
										+ ","
										+ String.valueOf(status
												.getGeoLocation()
												.getLongitude()));
						doc.put("lang", status.getLang());
						doc.put("place", status.getPlace());
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
			}
		}

	}

}
