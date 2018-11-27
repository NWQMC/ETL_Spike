package gov.acwi.wqp.etl;

import java.util.Map;

import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

public class MapItemSqlParameterSourceProvider implements ItemSqlParameterSourceProvider<Map<String, Object>> {

	public SqlParameterSource createSqlParameterSource(Map<String, Object> item) {
		return new MapSqlParameterSource(item);
	}

}
