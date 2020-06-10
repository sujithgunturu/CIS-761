import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
	private static Properties configProps = new Properties();

	private static String MySqlServerDriver;
	private static String MySqlServerUrl;
    private static String MySqlServerUser;
	private static String MySqlServerPassword;
	
	private static String PsotgreSqlServerDriver;
	private static String PostgreSqlServerUrl;
	private static String PostgreSqlServerUser;
	private static String PostgreSqlServerPassword;


	// DB Connection
	private Connection _mySqlDB; //IMDB
    private Connection _postgreSqlDB; //customer_DB

	// Canned queries

	private String _search_sql = "SELECT * FROM movie_info WHERE movie_name like ? ORDER BY movie_id";
	private PreparedStatement _search_statement;

	private String _producer_id_sql = "SELECT y.* "
					 + "FROM producer_movies x, producer_ids y "
					 + "WHERE x.movie_id = ? and x.producer_id = y.producer_id";
	private PreparedStatement _producer_id_statement;
	
	private String _actor_id_sql =  "SELECT y.* "
					 + "FROM actor_movies x, actor_ids y "
					 + "WHERE x.movie_id = ? and x.actor_id = y.actor_id";
	private PreparedStatement _actor_id_statement;


	/* uncomment, and edit, after your create your own customer database
uncommented 
	 */
	private String _customer_login_sql = "SELECT * FROM customer WHERE login = ? and password = ?";
	private PreparedStatement _customer_login_statement;

	private String _begin_transaction_read_write_sql = "START TRANSACTION";
	private PreparedStatement _begin_transaction_read_write_statement;

	private String _commit_transaction_sql = "COMMIT";
	private PreparedStatement _commit_transaction_statement;

	private String _rollback_transaction_sql = "ROLLBACK";
	private PreparedStatement _rollback_transaction_statement;
	
	 /* my querries sujith*/
	//remaining rentals
	private String remaining_movies_sql = "SELECT ("
			+ "(SELECT p.max_movies FROM plan p WHERE p.pid = c.pid) - "
			+ "(SELECT count(*) FROM rental r WHERE r.cid = c.cid AND r.status = 'open')) "
			+ "FROM customer c WHERE c.cid = ?";
	private PreparedStatement remainingmoviesprepstat;
	 //customer name 
	 private   String get_customer_name_SQL = "SELECT fname, lname FROM customer WHERE cid = ?";
	 private PreparedStatement customernameprepstat;
	//valid plan
	private String is_plan_valid_sql = "SELECT pid FROM plan WHERE pid = ?";
	private PreparedStatement isvalidplanprepstat;
	//valid movie
	private String is_movie_valid_sql = "SELECT movie_id FROM movie_info WHERE movie_id = ?";
	private PreparedStatement isvalidmovieprepstat;
	//who has movie 
	private String who_has_movie_sql = "SELECT cid "
			+ "FROM rental " + "WHERE mid = ? AND status = 'open'";
	private PreparedStatement whohasthemovieprepstat;
	 //list plans
	private String list_of_plans_SQL = "SELECT * FROM plan";
	private PreparedStatement listofplansprepstat;
	//update
	private String update_customer_plan_SQL = "UPDATE customer SET pid = ? WHERE cid = ?";
	private PreparedStatement updatePlanprepstat;
	//rent 
	//grade assignment
	private String RENT_A_MOVIE_SQL = "INSERT INTO rental(mid, cid, date_out, status) VALUES(?, ?, CURRENT_TIMESTAMP, 'open')";
	private PreparedStatement rentprepstat;
	//return 
	//grade assignment
	private String RETURN_A_MOVIE_SQL = "UPDATE rental SET status = 'closed' WHERE cid = ? AND mid = ?";
	private PreparedStatement returnprepstat;

	//fast search commands:
	private String FASTSEARCH1_SQL = "SELECT * FROM movie_info WHERE movie_name like ? ORDER BY movie_id";
	private PreparedStatement fastsearchprepstat1;

	
	private String FASTSEARCH2_SQL = "SELECT md.movie_id , d.producer_name FROM movie_info m, producer_movies md, producer_ids d WHERE m.movie_id = md.movie_id  AND md.producer_id  = d.producer_id  AND m.movie_name like ? ORDER BY md.movie_id";

	private PreparedStatement fastsearchprepstat2;

	private String FASTSEARCH3_SQL = "SELECT c.movie_id, a.actor_name FROM movie_info m, actor_movies c, actor_ids a WHERE m.movie_id  = c.movie_id AND c.actor_id = a.actor_id  AND m.movie_name LIKE ? ORDER BY c.movie_id";

	private PreparedStatement fastsearchprepstat3;

	public Query() {
	}

    /**********************************************************/
    /* Connection to MySQL database */

	public void openConnections() throws Exception {
        
        /* open connections to TWO databases: movie and  customer databases */
        
		configProps.load(new FileInputStream("dbconn.config"));
        
		MySqlServerDriver    = configProps.getProperty("MySqlServerDriver");
		MySqlServerUrl 	   = configProps.getProperty("MySqlServerUrl");
		MySqlServerUser 	   = configProps.getProperty("MySqlServerUser");
		MySqlServerPassword  = configProps.getProperty("MySqlServerPassword");
        
        PsotgreSqlServerDriver    = configProps.getProperty("PostgreSqlServerDriver");
        PostgreSqlServerUrl 	   = configProps.getProperty("PostgreSqlServerUrl");
        PostgreSqlServerUser 	   = configProps.getProperty("PostgreSqlServerUser");
        PostgreSqlServerPassword  = configProps.getProperty("PostgreSqlServerPassword");
                              
		/* load jdbc driver for MySQL */
		Class.forName(MySqlServerDriver).newInstance();

		/* open a connection to your mySQL database that contains the movie database */
		_mySqlDB = DriverManager.getConnection(MySqlServerUrl, // database
				MySqlServerUser, // user
				MySqlServerPassword); // password
		
     
        /* load jdbc driver for PostgreSQL */
        Class.forName(PsotgreSqlServerDriver).newInstance();
        
         /* connection string for PostgreSQL */
        String PostgreSqlConnectionString = PostgreSqlServerUrl+"?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&user="+
        		PostgreSqlServerUser+"&password=" + PostgreSqlServerPassword;
        
        
        /* open a connection to your postgreSQL database that contains the customer database */
        _postgreSqlDB = DriverManager.getConnection(PostgreSqlConnectionString);
        
	
	}

	public void closeConnections() throws Exception {
		_mySqlDB.close();
        _postgreSqlDB.close();
	}

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {

		_search_statement = _mySqlDB.prepareStatement(_search_sql);
		_producer_id_statement = _mySqlDB.prepareStatement(_producer_id_sql);
		_actor_id_statement = _mySqlDB.prepareStatement(_actor_id_sql);

		/* fast search prepared statements*/

		fastsearchprepstat1 = _mySqlDB.prepareStatement(FASTSEARCH1_SQL);
		fastsearchprepstat2 = _mySqlDB.prepareStatement(FASTSEARCH2_SQL);
		fastsearchprepstat3 = _mySqlDB.prepareStatement(FASTSEARCH3_SQL);


		/* uncomment after you create your customers database */
		
		_customer_login_statement = _postgreSqlDB.prepareStatement(_customer_login_sql);
		_begin_transaction_read_write_statement = _postgreSqlDB.prepareStatement(_begin_transaction_read_write_sql);
		_commit_transaction_statement = _postgreSqlDB.prepareStatement(_commit_transaction_sql);
		_rollback_transaction_statement = _postgreSqlDB.prepareStatement(_rollback_transaction_sql);
		

		/* add here more prepare statements for all the other queries you need */
		/* my querries prepared statements  sujith*/
		
		//_mySqlDB
		//listofplansprepstat = _mySqlDB.prepareStatement(list_of_plans_SQL);
		//updatePlanprepstat = _mySqlDB.prepareStatement(update_customer_plan_SQL);
		//rentprepstat = _mySqlDB.prepareStatement(RENT_A_MOVIE_SQL);
		//returnprepstat = _mySqlDB.prepareStatement(RETURN_SQL);
		//customernameprepstat = _mySqlDB.prepareStatement(get_customer_name_SQL);
		//isvalidplanprepstat = _mySqlDB.prepareStatement(is_plan_valid_sql);
		isvalidmovieprepstat = _mySqlDB.prepareStatement(is_movie_valid_sql);
		//whohasthemovieprepstat = _mySqlDB.prepareStatement(who_has_movie_sql);
		//remainingmoviesprepstat = _mySqlDB.prepareStatement(remaining_movies_sql);
		
		//_postgreSqlDB
		customernameprepstat = _postgreSqlDB.prepareStatement(get_customer_name_SQL);
		listofplansprepstat= _postgreSqlDB.prepareStatement(list_of_plans_SQL);
		updatePlanprepstat = _postgreSqlDB.prepareStatement(update_customer_plan_SQL);
		rentprepstat = _postgreSqlDB.prepareStatement(RENT_A_MOVIE_SQL);
		returnprepstat = _postgreSqlDB.prepareStatement(RETURN_A_MOVIE_SQL);
		isvalidplanprepstat = _postgreSqlDB.prepareStatement(is_plan_valid_sql);
		//isvalidmovieprepstat = _postgreSqlDB.prepareStatement(is_movie_valid_sql);
		whohasthemovieprepstat = _postgreSqlDB.prepareStatement(who_has_movie_sql);
		remainingmoviesprepstat = _postgreSqlDB.prepareStatement(remaining_movies_sql);
	}


    /**********************************************************/
    /* suggested helper functions  */

	public int helper_compute_remaining_rentals(int cid) throws Exception {
	
		remainingmoviesprepstat.clearParameters();
		remainingmoviesprepstat.setInt(1, cid);

		ResultSet rentalsLeftSet = remainingmoviesprepstat.executeQuery();
		rentalsLeftSet.next();
		int c = rentalsLeftSet.getInt(1);

		rentalsLeftSet.close();
		return c;
	}

	public String helper_compute_customer_name(int cid) throws Exception {
		String name = null;
		customernameprepstat.clearParameters();
		customernameprepstat.setInt(1, cid);

		ResultSet nameSet = customernameprepstat.executeQuery();
		nameSet.next();
		name = nameSet.getString("fname") + " "+ nameSet.getString("lname");
		nameSet.close();
		return name;
		

	}

	public boolean helper_check_plan(int plan_id) throws Exception {
		isvalidplanprepstat.clearParameters();
		isvalidplanprepstat.setInt(1, plan_id);
		ResultSet validSet = isvalidplanprepstat.executeQuery();
		if(validSet.next()){
			validSet.close();
			return true;
		}
		else {
		validSet.close();
		return false;
		}
	}

	public boolean helper_check_movie(String movie_id) throws Exception {
		isvalidmovieprepstat.clearParameters();
		isvalidmovieprepstat.setString(1, movie_id);
		ResultSet validSet = isvalidmovieprepstat.executeQuery();
		if(validSet.next()){
			validSet.close();
			return true;
		}
		else {
			validSet.close();
			return false;
		}
	}

	private int helper_who_has_this_movie(String movie_id) throws Exception {
		/* find the customer id (cid) of whoever currently rents the movie movie_id; return -1 if none */
		whohasthemovieprepstat.clearParameters();
		whohasthemovieprepstat.setString(1, movie_id);

		ResultSet rentsSet = whohasthemovieprepstat.executeQuery();
		int cid;
		if (rentsSet.next()) {
			cid = rentsSet.getInt("cid");
		} else {
			cid = -1;
		}
		rentsSet.close();
		return cid;
	}

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
	public int transaction_login(String name, String password) throws Exception {		
		int cid;
		_customer_login_statement.clearParameters();
		_customer_login_statement.setString(1,name);
		_customer_login_statement.setString(2,password);
	    ResultSet cid_set = _customer_login_statement.executeQuery();
	    if (cid_set.next()) cid = cid_set.getInt(1);
		else cid = -1;
		return(cid);
		
		
	}

	public void transaction_personal_data(int cid) throws Exception {
		/* println the customer's personal data: name and number of remaining rentals */
		System.out.println("Hiii, " + helper_compute_customer_name(cid));
		System.out.println("You can rent " + helper_compute_remaining_rentals(cid)+ " more movies.");
	}


    /**********************************************************/
    /* main functions in this project: */

	public void transaction_search(int cid, String movie_name)
			throws Exception {
		_search_statement.clearParameters();
		_search_statement.setString(1, '%' + movie_name + '%');

		ResultSet movie_set = _search_statement.executeQuery();
		while (movie_set.next()) {
			String movie_id = movie_set.getString(1);
			System.out.println("ID: " + movie_id + " NAME: "
					+ movie_set.getString(2) + " YEAR: "
					+ movie_set.getString(3) + " RATING: "
					+ movie_set.getString(4));
			/* do a dependent join with producer */
			_producer_id_statement.clearParameters();
			_producer_id_statement.setString(1, movie_id);
			ResultSet producer_set = _producer_id_statement.executeQuery();
			while (producer_set.next()) {
				System.out.println("\t\tProducer name: " + producer_set.getString(2));
			}
			producer_set.close();
			_actor_id_statement.clearParameters();
			_actor_id_statement.setString(1, movie_id);
			ResultSet actor_set = _actor_id_statement.executeQuery();
			while(actor_set.next()){
			System.out.println("\t\tactor name:" + actor_set.getString(2));
			 }
			int hasMovie = helper_who_has_this_movie(movie_id);
			if (hasMovie == -1)
				System.out.println("\t\tAVAILABLE");
			else if (hasMovie == cid)
				System.out.println("\t\tYOU HAVE IT");
			else
				System.out.println("\t\tUNAVAILABLE");

		}
		System.out.println();
	}

	public void transaction_choose_plan(int cid, int pid) throws Exception {
			
			//_begin_transaction_read_write_statement.executeQuery();
			_begin_transaction_read_write_statement.executeUpdate();
			updatePlanprepstat.clearParameters();
			updatePlanprepstat.setInt(1, pid);
			updatePlanprepstat.setInt(2, cid);
			updatePlanprepstat.executeUpdate();
			int remaining = helper_compute_remaining_rentals(cid);

			if (remaining < 0) {
				_rollback_transaction_statement.executeUpdate();
				System.out.println("sorry cant change please return few movies");
			} else {
				System.out.println("you have successfully switched your  movie plan");
				_commit_transaction_statement.executeUpdate();
			}
	}

	public void transaction_list_plans() throws Exception {

		ResultSet plansSet = listofplansprepstat.executeQuery();
		while (plansSet.next()) {
			System.out.printf("%d\t%-20s\tmaximum_movies %d\t%.2f",
					plansSet.getInt("pid"), plansSet.getString("name"),
					plansSet.getInt("max_movies"), plansSet.getDouble("fee"));
			System.out.println();
		}
		plansSet.close();
	}

	public void transaction_rent(int cid, String movie_id) throws Exception
	{
		
			_begin_transaction_read_write_statement.executeUpdate();
			int remaining = helper_compute_remaining_rentals(cid);
			int hasMovie = helper_who_has_this_movie(movie_id);
			boolean ismovievalid = helper_check_movie(movie_id);
			if (remaining <= 0) {
				_rollback_transaction_statement.executeUpdate();
				System.out.println("You cannot rent because maximum movies are over ");
				return;
			}

			if (hasMovie == -1 && ismovievalid) {
				rentprepstat.clearParameters();
				rentprepstat.setString(1, movie_id);
				rentprepstat.setInt(2, cid);
				rentprepstat.executeUpdate();
				_commit_transaction_statement.executeUpdate();
				return;
			}
			else if (!ismovievalid) {
			System.out.println("movie id is not valid movie");
			}
			_rollback_transaction_statement.executeUpdate();
			if (hasMovie == cid && ismovievalid)   {
				System.out.println("already taken by you");
			} else if(ismovievalid && hasMovie != cid) {
				System.out.println("other customer took");
			}
	
	
	}
	


	

	public void transaction_return(int cid, String movie_id) throws Exception {
	
			_begin_transaction_read_write_statement.executeUpdate();
			int hasMovie = helper_who_has_this_movie(movie_id);
				

			if (hasMovie == cid) {
				returnprepstat.clearParameters();
				returnprepstat.setInt(1, cid);
				returnprepstat.setString(2, movie_id);
				returnprepstat.executeUpdate();
				_commit_transaction_statement.executeUpdate();
				return;
			}
			_rollback_transaction_statement.executeUpdate();
			System.out.println("Not a valid or you may not have rented this movie");
		
	}

	public void transaction_fast_search(int cid, String movie_name) throws Exception {

			
    	fastsearchprepstat1.clearParameters();
		fastsearchprepstat1.setString(1, '%' + movie_name +'%');
		ResultSet fs1 = fastsearchprepstat1.executeQuery();

		fastsearchprepstat2.clearParameters();
		fastsearchprepstat2.setString(1, '%' + movie_name +'%');
		ResultSet fs2 = fastsearchprepstat2.executeQuery();
		
		fastsearchprepstat3.clearParameters();
		fastsearchprepstat3.setString(1, '%' + movie_name +'%');
		ResultSet fs3 = fastsearchprepstat3.executeQuery();

		boolean fs2next = fs2.next();
		boolean fs3next = fs3.next();

		while(fs1.next()){
			String mid = fs1.getString(1);
			System.out.println("ID: " + mid + " NAME: " + fs1.getString(2)
					+ " YEAR: " + fs1.getInt(3));

			while (fs2next && !fs2.getString(1).equals(mid))
				fs2next = fs2.next();
			while (fs2next && fs2.getString(1).equals(mid)) {
				System.out.println("\t\tProducer: " + fs2.getString(2));
				fs2next = fs2.next();
			}

			while (fs3next && !fs3.getString(1).equals(mid))
				fs3next = fs3.next();
			while (fs3next && fs3.getString(1).equals(mid)) {
				System.out.println("\t\tActor: " + fs3.getString(2));
				fs3next = fs3.next();
			}
		}

				
		   

	fs1.close();
	fs2.close();
	fs3.close();
		  
	}

}
