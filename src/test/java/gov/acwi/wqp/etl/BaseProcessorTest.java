package gov.acwi.wqp.etl;

import org.junit.jupiter.api.BeforeEach;

//TODO - ??
public abstract class BaseProcessorTest {

//	public static final String SPACES = "   ";

	public static final Integer TEST_DATA_SOURCE_ID = 3;
	public static final String TEST_DATA_SOURCE = "STORET";

//	public static final String TEST_ORG_ID = "ARS";
//	public static final String TEST_ORG_NAME = "USDA Agricultural Research Service";
//
//	public static final Integer TEST_PROJECT_ID = 1;
//	public static final String TEST_PROJECT_IDENTIFIER = "CEAP";
//	public static final String TEST_PROJECT_NAME = "Conservation Effects Assessment Program";
//	public static final String TEST_PROJECT_DESCRIPTION = "CEAP Description";
//
//	public static final String TEST_ML_ID = "INSJ-INSJADE";
//	public static final String TEST_ML_NAME = "INSJADE";
//	public static final String TEST_ML_TYPE_NAME = "Land Runoff";
//	public static final String TEST_ML_DESCRIPTION = "INSJADE s located in a paired agricultural depress";
//	public static final String TEST_HUC_8 = "04100003";
//	public static final String TEST_HUC_12 = "041000030603";
//	public static final String TEST_DRAIN_AREA = "4.9";
//	public static final String TEST_DRAIN_AREA_UNIT = "ha";
//	public static final String TEST_LATITUDE = "41.4723060652";
//	public static final String TEST_LONGITUDE = "-84.990896451";
//	public static final String TEST_H_METHOD = "GPS Code (Pseudo Range) Differential";
//	public static final String TEST_H_REF_SYSTEM = "NAD83";
//	public static final String TEST_COUNTRY = "US";
//	public static final String TEST_STATE = "18";
//	public static final String TEST_COUNTY = "033";
//
//	public static final Integer TEST_STATION_ID = 13;
//	public static final String TEST_SITE_ID = "ARS-INSJ-INSJADE";
//	public static final String TEST_SITE_TYPE = "Land";
//	public static final String TEST_GOVERNMENT_UNIT_CODE = "US:18:033";
//	public static final BigDecimal TEST_DECIMAL_LATITUDE = new BigDecimal(TEST_LATITUDE);
//	public static final BigDecimal TEST_DECIMAL_LONGITUDE = new BigDecimal(TEST_LONGITUDE);
//	public static final BigDecimal TEST_DECIMAL_DRAIN_AREA = new BigDecimal(TEST_DRAIN_AREA);
//	public static final PGgeometry TEST_GEOM = MonitoringLocation
//			.calculateGeom(new BigDecimal(TEST_LATITUDE),
//					new BigDecimal(TEST_LONGITUDE),
//					MonitoringLocation.DEFAULT_SRID);
//
//
//	public static final String TEST_ACTIVITY_IDENTIFIER = "INSJALG_20070101";
//	public static final String TEST_ACTIVITY_TYPE_CODE = "Field Msr/Obs";
//	public static final String TEST_ACTIVITY_MEDIA_NAME = "Water";
//	public static final String TEST_ACTIVITY_START_DATE = "1/1/2007";
//	public static final String TEST_ACTIVITY_START_TIME = "10:50:00";
//	public static final String TEST_ACTIVITY_START_TIMEZONE = "EST";
//	public static final String TEST_ACTIVITY_COMMENT = "ActivityCommentText";
//	public static final String TEST_SAMPLE_COLLECTION_METHOD_ID = "INSJ_FM1";
//	public static final String TEST_SAMPLE_COLLECTION_CONTEXT = "ARS Methods Catalog";
//	public static final String TEST_SAMPLE_COLLECTION_NAME = "Stream Level Measurement";
//	public static final String TEST_SAMPLE_COLLECTION_DESCRIPTION = "SampleDescriptionMethodDescriptionText";
//	public static final String TEST_SAMPLE_COLLECTION_EQUIPMENT = "SampleCollectionEquipmentName";
//	public static final String TEST_SAMPLE_COLLECTION_EQUIPMENT_COMMENT = "SampleCollectionEquipmentCommentText";
//
//	public static final Integer TEST_ACTIVITY_ID = 39;
//	public static final LocalDate TEST_EVENT_DATE_LOCAL_DATE = LocalDate
//			.parse(TEST_ACTIVITY_START_DATE, Application.ARS_DATE_TIME_FORMATTER);
//
//
//	public static final String TEST_RESULT_DETECTION_CONDITION = "Not Reported";
//	public static final String TEST_CHARACTERISTIC_NAME = "Phosphorus";
//	public static final String TEST_SAMPLE_FRACTION_TEXT = "Total";
//	public static final String TEST_RESULT_MEASURE_VALUE = "0.143";
//	public static final String TEST_RESULT_MEASURE_UNIT = "mg/L";
//	public static final String TEST_RESULT_STATUS_ID = "Final";
//	public static final String TEST_RESULT_VALUE_TYPE_NAME = "Actual";
//	public static final String TEST_DATA_QUALITY_PRECISION = "10 %";
//	public static final String TEST_RESULT_COMMENT = "Result Comment";
//	public static final String TEST_ANALYTICAL_METHOD_ID = "INSJ_WQ9";
//	public static final String TEST_ANALYTICAL_METHOD_CONTEXT = "ARS Methods Catalog";
//	public static final String TEST_ANALYTICAL_METHOD_NAME = "ortho phosphate in water";
//	public static final String TEST_ANALYTICAL_METHOD_DESCRIPTION = "https://www.nrrig.mwa.ars.usda.gov/st40_javascript/methods.aspx?methodstring=INSJ_WQ9";
//	public static final String TEST_DETECTION_QUANTITATION_LIMIT_NAME = "Instrument Detection Level";
//	public static final String TEST_DETECTION_QUANTITATION_LIMIT_VALUE = "0.05";
//	public static final String TEST_DETECTION_QUANTITATION_LIMIT_UNIT = "mg/L";
//
//	public static final Integer TEST_RESULT_ID = 26;
//	public static final String TEST_CHARACTERISTIC_TYPE = "Nutrient";

	protected ConfigurationService configurationService;

	@BeforeEach
	public void setup() {
		configurationService = new ConfigurationService();
		configurationService.setEtlDataSourceId(TEST_DATA_SOURCE_ID);
		configurationService.setEtlDataSource(TEST_DATA_SOURCE);
	}

}
