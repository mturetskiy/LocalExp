import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class FinalConsumer {
    final static Logger log = LoggerFactory.getLogger(FinalConsumer.class);

    public void doFinalProcess(List<String> events) {
        log.info("Final events processing. {} events: {}", events.size(), events);

        String result = events.stream().map(DigestUtils::sha512Hex).collect(Collectors.joining(":"));
        log.info("Calculated final result: {}", DigestUtils.sha1Hex(result));
    }
}
