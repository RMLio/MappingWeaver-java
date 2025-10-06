package be.ugent.idlab.knows.cores;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Testcontainers
public abstract class DBTestCore extends TestCore {

    protected final String USERNAME;
    protected final String PASSWORD;
    protected final String DOCKER_TAG;

    protected DBTestCore(String username, String password, String dockerTag) {
        this.USERNAME = username;
        this.PASSWORD = password;
        this.DOCKER_TAG = dockerTag;
    }

    protected void prepareDatabase(String path) {
        try(Connection connection = DriverManager.getConnection(this.getDbURL(), this.USERNAME, this.PASSWORD)) {
            ScriptRunner runner = new ScriptRunner(connection);
            Reader reader = new BufferedReader(new FileReader(path));
            runner.setLogWriter(null); // ScriptRunner will output the contents of the SQL file to System.out by default

            runner.runScript(reader);
        } catch (SQLException | FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract String getDbURL();
}
