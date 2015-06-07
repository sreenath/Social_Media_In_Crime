/*
 * HARVEST USING STREAMING API AND READ TIMELINE
 *   Program to stream tweets from Melbourne
 *   Identify the tweeter screen name
 *   Iteratively read his timeline
 *   Duplicate Handling Performed 
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Locale;

import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;

import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Document;
import com.fourspaces.couchdb.Session;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class StreamTimeLine {

	static double dSouthWesternLongitude = 140.962477;
	static double dSouthWesternLatitude = -39.224731;

	static double dNorthEasternLongitude = 149.976488;
	static double dNorthEasternLatitude = -33.981051;

	public static void main(String[] args) {
		Session dbSession = new Session("localhost", 5984);

		if (!dbSession.getDatabaseNames().contains("crime_tweets")) {
			dbSession.createDatabase("crime_tweets");
		}
		final Database db = dbSession.getDatabase("crime_tweets");
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();

		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "crime").build();

		final Client client = new TransportClient(settings)
				.addTransportAddress(
						new InetSocketTransportAddress("146.118.97.20", 9300))
				.addTransportAddress(
						new InetSocketTransportAddress("146.118.96.247", 9300));

		double[][] loc = { { 144.394492, -37.860282 },
				{ 145.764740, -37.459846 } };
		FilterQuery filter = new FilterQuery();
		filter.locations(loc);
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				if (!status.isRetweet()) {

					// Check for duplicate
					CountResponse response = client.prepareCount("tweets*")
							.setQuery(termQuery("id", status.getId()))
							.execute().actionGet();

					if (response.getCount() == 0) {
						writeTweetToDocument(status);

						String sUserName = status.getUser().getScreenName();

						ResponseList<Status> statuses = null;
						Paging page = new Paging(1, 200);
						for (int i = 1; i < 200; i++) {
							try {
								page.setPage(i);
								Twitter twitter = new TwitterFactory()
										.getInstance();
								statuses = twitter.getUserTimeline(sUserName,
										page);

								for (Status StatuS : statuses) {

									if (!StatuS.isRetweet()) {
										boolean bPostedFromVictoria = false;
										if (StatuS.getGeoLocation() == null)
											bPostedFromVictoria = true;
										else {
											boolean bWithinLatitude = StatuS
													.getGeoLocation()
													.getLatitude() >= dSouthWesternLatitude
													&& StatuS.getGeoLocation()
															.getLatitude() <= dNorthEasternLatitude;
											boolean bWithinLongitude = StatuS
													.getGeoLocation()
													.getLongitude() >= dSouthWesternLongitude
													&& StatuS.getGeoLocation()
															.getLongitude() <= dNorthEasternLongitude;
											bPostedFromVictoria = bWithinLatitude
													&& bWithinLongitude;
										}

										if (bPostedFromVictoria) {
											// check for duplicate
											CountResponse tweetResponse = client
													.prepareCount("tweets*")
													.setQuery(
															termQuery(
																	"id",
																	StatuS.getId()))
													.execute().actionGet();
											if (tweetResponse.getCount() == 0) {
												writeTweetToDocument(StatuS);
											} else {

												/*
												 * Assuming that if there are
												 * more than 50 tweets from same
												 * user, his timeline has been
												 * read before.
												 */

												CountResponse userTweetCount = client
														.prepareCount("tweets*")
														.setQuery(
																termQuery(
																		"UserId",
																		StatuS.getUser()
																				.getId()))
														.execute().actionGet();
												if (userTweetCount.getCount() > 50) {
													break;
												}
											}
										}
									}
								}
							} catch (Exception ex) {
							}
						}
					}
				}
			}

			void writeTweetToDocument(Status StatuS) {
				Document doc = new Document();
				String id = String.valueOf(StatuS.getId());
				doc.setId(id);
				doc.put("id", id);
				doc.put("UserId", StatuS.getUser().getId());
				doc.put("user", StatuS.getUser().getScreenName());
				doc.put("text", StatuS.getText());
				doc.put("created_at", StatuS.getCreatedAt());

				GeoLocation gl = StatuS.getGeoLocation();
				if (gl != null) {
					String sLocation = gl.getLatitude() + ","
							+ gl.getLongitude();
					doc.put("location", sLocation);
				}
				doc.put("lang", StatuS.getLang());
				doc.put("place", StatuS.getPlace());

				try {
					db.saveDocument(doc);
					System.out.println(doc.toString());
				} catch (Exception ex) {
					System.out
							.println("Possible duplicate: " + ex.getMessage());
				}
			}

			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:"
						+ statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:"
						+ numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId
						+ " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		twitterStream.addListener(listener);
		twitterStream.filter(filter);
		client.threadPool().shutdown();
	}

}
