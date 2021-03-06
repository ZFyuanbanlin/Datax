package com.alibaba.datax.plugin.writer.hologresjdbcwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.hologresjdbcwriter.util.ConfLoader;
import com.alibaba.datax.plugin.writer.hologresjdbcwriter.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.writer.hologresjdbcwriter.util.WriterUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import com.alibaba.hologres.client.model.TableSchema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BaseWriter {

	protected static final Set<String> ignoreConfList;

	static {
		ignoreConfList = new HashSet<>();
		ignoreConfList.add("jdbcUrl");
		ignoreConfList.add("username");
		ignoreConfList.add("password");
		ignoreConfList.add("writeMode");
	}

	enum WriteMode {
		IGNORE,
		UPDATE,
		REPLACE
	}

	private static WriteMode getWriteMode(String text) {
		text = text.toUpperCase();
		switch (text) {
			case "IGNORE":
				return WriteMode.IGNORE;
			case "UPDATE":
				return WriteMode.UPDATE;
			case "REPLACE":
				return WriteMode.REPLACE;
			default:
				throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, "writeMode?????????IGNORE,UPDATE,REPLACE,???????????? " + text);
		}
	}

	public static class Job {
		private DataBaseType dataBaseType;

		private static final Logger LOG = LoggerFactory
				.getLogger(BaseWriter.Job.class);

		public Job(DataBaseType dataBaseType) {
			this.dataBaseType = dataBaseType;
			OriginalConfPretreatmentUtil.DATABASE_TYPE = this.dataBaseType;
		}

		public void init(Configuration originalConfig) {
			OriginalConfPretreatmentUtil.doPretreatment(originalConfig, this.dataBaseType);
			checkConf(originalConfig);
			LOG.debug("After job init(), originalConfig now is:[\n{}\n]",
					originalConfig.toJSON());
		}

		private void checkConf(Configuration originalConfig) {
			getWriteMode(originalConfig.getString(Key.WRITE_MODE, "REPLACE"));
			List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
			List<JSONObject> conns = originalConfig.getList(Constant.CONN_MARK,
					JSONObject.class);
			if (conns.size() > 1) {
				throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, "?????????????????????");
			}
			int tableNumber = originalConfig.getInt(Constant.TABLE_NUMBER_MARK);
			if (tableNumber > 1) {
				throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, "?????????????????????");
			}
			JSONObject connConf = conns.get(0);
			String jdbcUrl = connConf.getString(Key.JDBC_URL);
			String username = originalConfig.getString(Key.USERNAME);
			String password = originalConfig.getString(Key.PASSWORD);

			String table = connConf.getJSONArray(Key.TABLE).getString(0);

			Map<String, Object> clientConf = originalConfig.getMap("client");

			HoloConfig config = new HoloConfig();
			config.setJdbcUrl(jdbcUrl);
			config.setUsername(username);
			config.setPassword(password);
			if (clientConf != null) {
				try {
					config = ConfLoader.load(clientConf, config, ignoreConfList);
				} catch (Exception e) {
					throw DataXException
							.asDataXException(
									DBUtilErrorCode.CONF_ERROR,
									"??????????????????.");
				}
			}

			try (HoloClient client = new HoloClient(config)) {
				TableSchema schema = client.getTableSchema(table);
				LOG.info("table {} column info:", schema.getTableNameObj().getFullName());
				for (com.alibaba.hologres.client.model.Column column : schema.getColumnSchema()) {
					LOG.info("name:{},type:{},typeName:{},nullable:{},defaultValue:{}", column.getName(), column.getType(), column.getTypeName(), column.getAllowNull(), column.getDefaultValue());
				}
				for (String userColumn : userConfiguredColumns) {
					if (schema.getColumnIndex(userColumn) == null) {
						throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR, "???????????? " + userColumn + " ?????????");
					}
				}
			} catch (Exception e) {
				throw DataXException.asDataXException(DBUtilErrorCode.CONN_DB_ERROR, "?????????schema??????", e);
			}

		}

		// ????????????????????????????????? task ?????????pre ?????????????????????????????????
		public void prepare(Configuration originalConfig) {

			try {
				String username = originalConfig.getString(Key.USERNAME);
				String password = originalConfig.getString(Key.PASSWORD);

				List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
						Object.class);
				Configuration connConf = Configuration.from(conns.get(0)
						.toString());

				String jdbcUrl = connConf.getString(Key.JDBC_URL);
				originalConfig.set(Key.JDBC_URL, jdbcUrl);

				String table = connConf.getList(Key.TABLE, String.class).get(0);
				originalConfig.set(Key.TABLE, table);

				List<String> preSqls = originalConfig.getList(Key.PRE_SQL,
						String.class);
				List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
						preSqls, table);

				originalConfig.remove(Constant.CONN_MARK);
				if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
					// ????????? preSql ???????????????????????????
					originalConfig.remove(Key.PRE_SQL);
					String tempJdbcUrl = jdbcUrl.replace("postgresql", "hologres");
					try (Connection conn = DriverManager.getConnection(
							tempJdbcUrl, username, password)) {
						LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
								StringUtils.join(renderedPreSqls, ";"), tempJdbcUrl);

						WriterUtil.executeSqls(conn, renderedPreSqls, tempJdbcUrl, dataBaseType);
					}
				}
				LOG.debug("After job prepare(), originalConfig now is:[\n{}\n]",
						originalConfig.toJSON());
			} catch (SQLException e) {
				throw DataXException.asDataXException(DBUtilErrorCode.SQL_EXECUTE_FAIL, e);
			}
		}

		public List<Configuration> split(Configuration originalConfig,
										 int mandatoryNumber) {
			return WriterUtil.doSplit(originalConfig, mandatoryNumber);
		}

		// ????????????????????????????????? task ?????????post ?????????????????????????????????
		public void post(Configuration originalConfig) {

			String username = originalConfig.getString(Key.USERNAME);
			String password = originalConfig.getString(Key.PASSWORD);

			String jdbcUrl = originalConfig.getString(Key.JDBC_URL);

			String table = originalConfig.getString(Key.TABLE);

			List<String> postSqls = originalConfig.getList(Key.POST_SQL,
					String.class);
			List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
					postSqls, table);

			if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {
				// ????????? postSql ???????????????????????????
				originalConfig.remove(Key.POST_SQL);
				String tempJdbcUrl = jdbcUrl.replace("postgresql", "hologres");
				Connection conn = DBUtil.getConnection(this.dataBaseType,
						tempJdbcUrl, username, password);

				LOG.info(
						"Begin to execute postSqls:[{}]. context info:{}.",
						StringUtils.join(renderedPostSqls, ";"), tempJdbcUrl);
				WriterUtil.executeSqls(conn, renderedPostSqls, tempJdbcUrl, dataBaseType);
				DBUtil.closeDBResources(null, null, conn);
			}

		}

		public void destroy(Configuration originalConfig) {
		}

	}

	public static class Task {
		protected static final Logger LOG = LoggerFactory
				.getLogger(BaseWriter.Task.class);

		protected DataBaseType dataBaseType;

		protected String username;
		protected String password;
		protected String jdbcUrl;
		protected String table;
		protected List<String> columns;
		protected int batchSize;
		protected int batchByteSize;
		protected int columnNumber = 0;
		protected TaskPluginCollector taskPluginCollector;

		// ??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
		protected static String BASIC_MESSAGE;

		protected WriteMode writeMode;
		protected String arrayDelimiter;
		protected boolean emptyAsNull;

		protected HoloConfig config;

		public Task(DataBaseType dataBaseType) {
			this.dataBaseType = dataBaseType;
		}

		public void init(Configuration writerSliceConfig) {
			this.username = writerSliceConfig.getString(Key.USERNAME);
			this.password = writerSliceConfig.getString(Key.PASSWORD);
			this.jdbcUrl = writerSliceConfig.getString(Key.JDBC_URL);
			this.table = writerSliceConfig.getString(Key.TABLE);

			this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
			this.columnNumber = this.columns.size();

			this.arrayDelimiter = writerSliceConfig.getString(Key.Array_Delimiter);

			this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
			this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);

			writeMode = getWriteMode(writerSliceConfig.getString(Key.WRITE_MODE, "REPLACE"));
			emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);

			Map<String, Object> clientConf = writerSliceConfig.getMap("client");

			config = new HoloConfig();
			config.setJdbcUrl(this.jdbcUrl);
			config.setUsername(username);
			config.setPassword(password);
			config.setWriteMode(writeMode == WriteMode.IGNORE ? com.alibaba.hologres.client.model.WriteMode.INSERT_OR_IGNORE : (writeMode == WriteMode.UPDATE ? com.alibaba.hologres.client.model.WriteMode.INSERT_OR_UPDATE : com.alibaba.hologres.client.model.WriteMode.INSERT_OR_REPLACE));
			config.setWriteBatchSize(this.batchSize);
			config.setWriteBatchTotalByteSize(this.batchByteSize);
			config.setMetaCacheTTL(3600000L);
			config.setEnableDefaultForNotNullColumn(false);
			config.setRetryCount(5);
			config.setAppName("datax");

			if (clientConf != null) {
				try {
					config = ConfLoader.load(clientConf, config, ignoreConfList);
				} catch (Exception e) {
					throw DataXException
							.asDataXException(
									DBUtilErrorCode.CONF_ERROR,
									"??????????????????.");
				}
			}

			BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]",
					this.jdbcUrl, this.table);
		}

		public void prepare(Configuration writerSliceConfig) {

		}

		public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector) {
			this.taskPluginCollector = taskPluginCollector;

			try (HoloClient client = new HoloClient(config)) {
				Record record;
				TableSchema schema = RetryUtil.executeWithRetry(() -> client.getTableSchema(this.table), 3, 5000L, true);
				while ((record = recordReceiver.getFromReader()) != null) {
					if (record.getColumnNumber() != this.columnNumber) {
						// ??????????????????????????????????????????????????????????????????????????????
						throw DataXException
								.asDataXException(
										DBUtilErrorCode.CONF_ERROR,
										String.format(
												"????????????????????????. ???????????????????????????????????????????????????:%s ??? ??????????????????????????????:%s ?????????. ????????????????????????????????????.",
												record.getColumnNumber(),
												this.columnNumber));
					}
					Put put = convertToPut(record, schema);
					if (null != put) {
						try {
							client.put(put);
						} catch (HoloClientWithDetailsException detail) {
							handleDirtyData(detail);
						}
					}
				}
				try {
					client.flush();
				} catch (HoloClientWithDetailsException detail) {
					handleDirtyData(detail);
				}
			} catch (Exception e) {
				throw DataXException.asDataXException(
						DBUtilErrorCode.WRITE_DATA_ERROR, e);
			}
		}

		private void handleDirtyData(HoloClientWithDetailsException detail) {
			for (int i = 0; i < detail.size(); ++i) {
				com.alibaba.hologres.client.model.Record failRecord = detail.getFailRecord(i);
				if (failRecord.getAttachmentList() != null) {
					for (Object obj : failRecord.getAttachmentList()) {
						taskPluginCollector.collectDirtyRecord((Record) obj, detail.getException(i));
					}
				}
			}
		}

		public void startWrite(RecordReceiver recordReceiver,
							   TaskPluginCollector taskPluginCollector) {
			startWriteWithConnection(recordReceiver, taskPluginCollector);
		}

		public void post(Configuration writerSliceConfig) {

		}

		public void destroy(Configuration writerSliceConfig) {
		}

		// ?????????????????????????????????columnNumber,resultSetMetaData
		protected Put convertToPut(Record record, TableSchema schema) {
			try {
				Put put = new Put(schema);
				put.getRecord().addAttachment(record);
				for (int i = 0; i < this.columnNumber; i++) {
					fillColumn(put, schema, schema.getColumnIndex(this.columns.get(i)), record.getColumn(i));
				}
				return put;
			} catch (Exception e) {
				taskPluginCollector.collectDirtyRecord(record, e);
				return null;
			}

		}

		protected void fillColumn(Put data, TableSchema schema, int index, Column column) throws SQLException {
			com.alibaba.hologres.client.model.Column holoColumn = schema.getColumn(index);
			switch (holoColumn.getType()) {
				case Types.CHAR:
				case Types.NCHAR:
				case Types.CLOB:
				case Types.NCLOB:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.NVARCHAR:
				case Types.LONGNVARCHAR:
					String value = column.asString();
					if (emptyAsNull && value != null && value.length() == 0) {
						data.setObject(index, null);
					} else {
						data.setObject(index, value);
					}
					break;

				case Types.SMALLINT:
					if (column.getByteSize() > 0) {
						data.setObject(index, column.asBigInteger().shortValue());
					} else if (emptyAsNull) {
						data.setObject(index, null);
					}
					break;
				case Types.INTEGER:
					if (column.getByteSize() > 0) {
						data.setObject(index, column.asBigInteger().intValue());
					} else if (emptyAsNull) {
						data.setObject(index, null);
					}
					break;
				case Types.BIGINT:
					if (column.getByteSize() > 0) {
						data.setObject(index, column.asBigInteger().longValue());
					} else if (emptyAsNull) {
						data.setObject(index, null);
					}
					break;
				case Types.NUMERIC:
				case Types.DECIMAL:
					if (column.getByteSize() > 0) {
						data.setObject(index, column.asBigDecimal());
					} else if (emptyAsNull) {
						data.setObject(index, null);
					}
					break;
				case Types.FLOAT:
				case Types.REAL:
					if (column.getByteSize() > 0) {
						data.setObject(index, column.asBigDecimal().floatValue());
					} else if (emptyAsNull) {
						data.setObject(index, null);
					}
					break;
				case Types.DOUBLE:
					if (column.getByteSize() > 0) {
						data.setObject(index, column.asDouble());
					} else if (emptyAsNull) {
						data.setObject(index, null);
					}
					break;
				case Types.TIME:
					if (column.getByteSize() > 0) {
						if (column instanceof LongColumn || column instanceof DateColumn) {
							data.setObject(index, new Time(column.asLong()));
						} else {
							data.setObject(index, column.asString());
						}
					} else if (emptyAsNull) {
						data.setObject(index, null);
					}
					break;
				case Types.DATE:
					if (column.getByteSize() > 0) {
						if (column instanceof LongColumn || column instanceof DateColumn) {
							data.setObject(index, column.asLong());
						} else {
							data.setObject(index, column.asString());
						}
					} else if (emptyAsNull) {
						data.setObject(index, null);
					}
					break;
				case Types.TIMESTAMP:
					if (column.getByteSize() > 0) {
						if (column instanceof LongColumn || column instanceof DateColumn) {
							data.setObject(index, new Timestamp(column.asLong()));
						} else {
							data.setObject(index, column.asString());
						}
					} else if (emptyAsNull) {
						data.setObject(index, null);
					}
					break;

				case Types.BINARY:
				case Types.VARBINARY:
				case Types.BLOB:
				case Types.LONGVARBINARY:
					String byteValue = column.asString();
					if (null != byteValue) {
						data.setObject(index, column
								.asBytes());
					}
					break;
				case Types.BOOLEAN:
				case Types.BIT:
					if (column.getByteSize() == 0) {
						break;
					}
					try {
						Boolean boolValue = column.asBoolean();
						data.setObject(index, boolValue);
					} catch (Exception e) {
						data.setObject(index, !"0".equals(column.asString()));
					}
					break;
				case Types.ARRAY:
					String arrayString = column.asString();
					Object arrayObject = null;
					if (null == arrayString || (emptyAsNull && "".equals(arrayString))) {
						data.setObject(index, null);
						break;
					} else if (arrayDelimiter != null && arrayDelimiter.length() > 0) {
						arrayObject = arrayString.split(this.arrayDelimiter);
					} else {
						arrayObject = JSONArray.parseArray(arrayString);
					}
					data.setObject(index, arrayObject);
					break;
				default:
					throw DataXException
							.asDataXException(
									DBUtilErrorCode.UNSUPPORTED_TYPE,
									String.format(
											"?????????????????????????????????????????????. ??????DataX ??????????????????????????????????????????. ?????????:[%s], ????????????:[%d], ??????Java??????:[%s]. ?????????????????????????????????????????????????????????.",
											holoColumn.getName(),
											holoColumn.getType(),
											holoColumn.getTypeName()));
			}
		}
	}
}
