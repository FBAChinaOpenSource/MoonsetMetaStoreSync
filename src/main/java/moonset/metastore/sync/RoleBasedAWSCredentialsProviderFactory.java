package moonset.metastore.sync;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.glue.catalog.metastore.AWSCredentialsProviderFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.UUID;

public class RoleBasedAWSCredentialsProviderFactory implements AWSCredentialsProviderFactory {

    private static final Log log = 
        LogFactory.getLog(RoleBasedAWSCredentialsProviderFactory.class);

    public static final String ASSUME_ROLE = "moonset.metastore.sync.assume.role";

    private static final String ROLE_SESSION_NAME_PREFIX = "MoonsetMetastoreSync";

    private static final int ROLE_SESSION_TIMEOUT_SECONDS = 3600; // Should be between 900 to 3600


    @Override
    public AWSCredentialsProvider buildAWSCredentialsProvider(Configuration configuration) {
        String assumeRole = configuration.get(ASSUME_ROLE);
        log.info("The assume role is " + assumeRole);
        return new STSAssumeRoleSessionCredentialsProvider.Builder(
                assumeRole, ROLE_SESSION_NAME_PREFIX +
                UUID.randomUUID().getMostSignificantBits()) // Using UUID to make the session name unique among multiple sessions
                .withRoleSessionDurationSeconds(ROLE_SESSION_TIMEOUT_SECONDS)
                .build();
    }
}
