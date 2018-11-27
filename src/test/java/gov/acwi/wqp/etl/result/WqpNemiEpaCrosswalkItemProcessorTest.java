package gov.acwi.wqp.etl.result;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import gov.acwi.wqp.etl.result.WqpNemiEpaCrosswalkItemProcessor;

public class WqpNemiEpaCrosswalkItemProcessorTest {

	protected WqpNemiEpaCrosswalkItemProcessor ip;

	@Before
	public void setup() {
		ip = new WqpNemiEpaCrosswalkItemProcessor();
	}

	@Test
	public void getNemiUrlAnalytical() {
		assertEquals(WqpNemiEpaCrosswalkItemProcessor.ANALYTICAL_URL_PREFIX + "123",
				ip.getNemiUrl("123", WqpNemiEpaCrosswalkItemProcessor.METHOD_TYPE_ANALYTICAL));
	}

	@Test
	public void getNemiUrlStatistical() {
		assertEquals(WqpNemiEpaCrosswalkItemProcessor.STATISTICAL_URL_PREFIX + "123",
				ip.getNemiUrl("123", WqpNemiEpaCrosswalkItemProcessor.METHOD_TYPE_STATISTICAL));
	}

	@Test
	public void getNemiUrlUnknownMethodType() {
		assertNull(ip.getNemiUrl("123", "sillyness"));
	}

	@Test
	public void processNoMethodIdOrType() {
		try {
			Map<String, Object> mapOut = ip.process(new HashMap<String, Object>());
			assertEquals(1, mapOut.size());
			assertTrue(mapOut.containsKey(WqpNemiEpaCrosswalkItemProcessor.NEMI_URL));
			assertNull(mapOut.get(WqpNemiEpaCrosswalkItemProcessor.NEMI_URL));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void processNoMethodId() {
		Map<String, Object> mapIn = new HashMap<String, Object>();
		mapIn.put(WqpNemiEpaCrosswalkItemProcessor.METHOD_TYPE, WqpNemiEpaCrosswalkItemProcessor.METHOD_TYPE_ANALYTICAL);
		try {
			Map<String, Object> mapOut = ip.process(mapIn);
			assertEquals(mapIn.size() + 1, mapOut.size());
			assertTrue(mapOut.containsKey(WqpNemiEpaCrosswalkItemProcessor.NEMI_URL));
			assertNull(mapOut.get(WqpNemiEpaCrosswalkItemProcessor.NEMI_URL));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void processNoMethodType() {
		Map<String, Object> mapIn = new HashMap<String, Object>();
		mapIn.put(WqpNemiEpaCrosswalkItemProcessor.METHOD_ID, "123");
		try {
			Map<String, Object> mapOut = ip.process(mapIn);
			assertEquals(mapIn.size() + 1, mapOut.size());
			assertTrue(mapOut.containsKey(WqpNemiEpaCrosswalkItemProcessor.NEMI_URL));
			assertNull(mapOut.get(WqpNemiEpaCrosswalkItemProcessor.NEMI_URL));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void processNullMethodType() {
		Map<String, Object> mapIn = new HashMap<String, Object>();
		mapIn.put(WqpNemiEpaCrosswalkItemProcessor.METHOD_ID, "123");
		mapIn.put(WqpNemiEpaCrosswalkItemProcessor.METHOD_TYPE, null);
		try {
			Map<String, Object> mapOut = ip.process(mapIn);
			assertEquals(mapIn.size() + 1, mapOut.size());
			assertTrue(mapOut.containsKey(WqpNemiEpaCrosswalkItemProcessor.NEMI_URL));
			assertNull(mapOut.get(WqpNemiEpaCrosswalkItemProcessor.NEMI_URL));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void processAllGood() {
		Map<String, Object> mapIn = new HashMap<String, Object>();
		mapIn.put(WqpNemiEpaCrosswalkItemProcessor.METHOD_ID, "123");
		mapIn.put(WqpNemiEpaCrosswalkItemProcessor.METHOD_TYPE, WqpNemiEpaCrosswalkItemProcessor.METHOD_TYPE_ANALYTICAL);
		try {
			Map<String, Object> mapOut = ip.process(mapIn);
			assertEquals(mapIn.size() + 1, mapOut.size());
			assertTrue(mapOut.containsKey(WqpNemiEpaCrosswalkItemProcessor.NEMI_URL));
			assertEquals(WqpNemiEpaCrosswalkItemProcessor.ANALYTICAL_URL_PREFIX + "123",
					mapOut.get(WqpNemiEpaCrosswalkItemProcessor.NEMI_URL));
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
