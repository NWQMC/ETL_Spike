package gov.acwi.wqp.etl.result;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.LinkedCaseInsensitiveMap;

import gov.acwi.wqp.etl.Application;

public class WqpNemiEpaCrosswalkItemProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {
	private final Logger LOG = LoggerFactory.getLogger(Application.class);

	public static final String NEMI_URL = "NEMI_URL";
	public static final String METHOD_ID = "method_id";
	public static final String METHOD_TYPE = "method_type";
	public static final String METHOD_TYPE_ANALYTICAL = "analytical";
	public static final String METHOD_TYPE_STATISTICAL = "statistical";
	public static final String ANALYTICAL_URL_PREFIX = "https://www.nemi.gov/methods/method_summary/";
	public static final String STATISTICAL_URL_PREFIX = "https://www.nemi.gov/methods/sams_method_summary/";

	@Override
	public Map<String, Object> process(final Map<String, Object> map) throws Exception {
		LinkedCaseInsensitiveMap<Object> newMap = new LinkedCaseInsensitiveMap<>();
		newMap.putAll(map);
		if (map.containsKey(METHOD_ID) && null != map.get(METHOD_ID) && map.containsKey(METHOD_TYPE)) {
			newMap.put(NEMI_URL, getNemiUrl(String.valueOf(map.get(METHOD_ID)), String.valueOf(map.get(METHOD_TYPE))));
		} else {
			newMap.put(NEMI_URL, null);
		}
		LOG.debug(newMap.toString());
		return newMap;
	}

	protected String getNemiUrl(String methodId, String methodType) {
		switch (methodType) {
		case METHOD_TYPE_ANALYTICAL:
			return ANALYTICAL_URL_PREFIX + methodId;
		case METHOD_TYPE_STATISTICAL:
			return STATISTICAL_URL_PREFIX + methodId;
		default:
			return null;
		}
	}

}
