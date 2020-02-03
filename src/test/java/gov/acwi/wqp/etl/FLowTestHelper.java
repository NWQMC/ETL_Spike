package gov.acwi.wqp.etl;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

public class FLowTestHelper {
	public static final String ACTIVITY_SWAP_STORET = "activity_swap_storet";
	public static final String RESULT_SWAP_STORET = "result_swap_storet";

	public static HashMap<Long, String> getStationIdtoGeomMap(JdbcTemplate jdbcTemplate, String tableName,
			String keyCol, String valueCol) {
		String sql = String.format("select %s, %s from %s", keyCol, valueCol, tableName);
		HashMap<Long, String> ret = new HashMap<>();

		SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(sql);
		while (sqlRowSet.next()) {
			Long key = sqlRowSet.getLong(keyCol);
			String value = sqlRowSet.getString(valueCol);
			if (ret.containsKey(key)) {
				assertEquals(ret.get(key), value);
			} else {
				ret.put(key, value);
			}
		}

		return ret;
	}

	public static void verifySwapStoretGeom(JdbcTemplate jdbcTemplate, String destTable) {
		HashMap<Long,String> destTableMap = getStationIdtoGeomMap(jdbcTemplate, destTable, "station_id", "geom");
		HashMap<Long,String> stationSwapStoretMap = getStationIdtoGeomMap(jdbcTemplate, "station_swap_storet", "station_id", "geom");

		for(Long stationId : destTableMap.keySet()) {
			long resultStationId = stationId;
			String geom = stationSwapStoretMap.get(resultStationId);
			assertNotNull(geom);
			assertEquals(destTableMap.get(stationId), geom);
		}
	}

	public static void verifyActivitySwapStoretGeom(JdbcTemplate jdbcTemplate) {
		verifySwapStoretGeom(jdbcTemplate, ACTIVITY_SWAP_STORET);
	}

	public static void verifyResultSwapStoretGeom(JdbcTemplate jdbcTemplate) {
		verifySwapStoretGeom(jdbcTemplate, RESULT_SWAP_STORET);
	}

}