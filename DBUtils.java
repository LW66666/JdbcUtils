package com.lw.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBUtils {

	private static final String DRIVER = "com.mysql.jdbc.Driver";
	private static final String URL = "jdbc:mysql://localhost:3306/bfoa?useUnicode=true&amp;characterEncoding=utf-8&amp;autoReconnect=true";
	private static final String NAME = "root";
	private static final String PWD = "123456";

	static {
		// 加载数据库驱动程序
		try {
			Class.forName(DRIVER);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取数据库连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	private static Connection getConn() throws SQLException {
		return DriverManager.getConnection(URL, NAME, PWD);
	}

	/**
	 * 执行查询语句 返回一条结果
	 * 
	 * @param sql
	 * @param args
	 * @return
	 */
	public static Map<String, Object> query(String sql, Object... args) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		Map<String, Object> map = null;// 一开始就new找不到就空指针
		try {
			// 2
			conn = getConn();
			// 3
			pst = conn.prepareStatement(sql);
			for (int i = 0; args != null && i < args.length; i++) {
				pst.setObject(i + 1, args[i]);
			}
			// 4
			rs = pst.executeQuery();
			if (rs.next()) {
				map = new HashMap<String, Object>();
				rsToMap(rs, map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(conn, pst, rs);
		}
		return map;
	}

	/**
	 * 执行查询语句 返回多条结果
	 * 
	 * @param sql
	 * @param args
	 * @return
	 */
	public static List<Map<String, Object>> list(String sql, Object... args) {
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			// 2
			conn = getConn();
			// 3
			pst = conn.prepareStatement(sql);
			for (int i = 0; args != null && i < args.length; i++) {
				pst.setObject(i + 1, args[i]);
			}
			// 4
			rs = pst.executeQuery();
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				// 5
				rsToMap(rs, map);
				list.add(map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(conn, pst, rs);
		}
		return list;
	}

	/**
	 * 将rs结果集转换成map对象
	 * 
	 * @param rs
	 * @param map
	 * @throws SQLException
	 */
	private static void rsToMap(ResultSet rs, Map<String, Object> map)
			throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int count = rsmd.getColumnCount();
		for (int i = 1; i <= count; i++) {
			String key = rsmd.getColumnName(i).toLowerCase();
			Object value = rs.getObject(i);
			map.put(key, value);
		}
	}

	/**
	 * 执行增删改SQL语句
	 * 
	 * @param sql
	 * @param args
	 * @return true成功
	 */
	public static boolean update(String sql, Object... args) {
		PreparedStatement pst = null;
		Connection conn = null;
		try {
			// 2
			conn = getConn();
			// 3
			pst = conn.prepareStatement(sql);
			for (int i = 0; args != null && i < args.length; i++) {
				pst.setObject(i + 1, args[i]);
			}
			// 4
			int retInt = pst.executeUpdate();
			// 5
			return retInt > 0;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			// 6
			close(conn, pst, null);
		}
		return false;
	}

	/**
	 * 关闭链接释放资源
	 * 
	 * @param conn
	 * @param st
	 * @param rs
	 */
	private static void close(Connection conn, Statement st, ResultSet rs) {
		try {
			if (rs != null)
				rs.close();
			if (st != null)
				st.close();
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
