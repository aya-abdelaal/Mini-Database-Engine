package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import Exceptions.DBAppException;

public class Utilities {

	public Utilities() {
		// TODO Auto-generated constructor stub
	}

	public static int compareToDates(Object date1, Object date2) throws DBAppException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");

		Date one = null;
		Date two = null;

		if (date1 instanceof Date) {
			one = (Date) date1;
		} else {
			if (date1 instanceof String) {
				try {
					one = sdf.parse( ((String) date1).replace('-', '/') );
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		if (date2 instanceof String)
			try {
				two = sdf.parse(((String) date2).replace('-', '/'));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		else {
			if (date2 instanceof Date)
				two = (Date) date2;
		}

		return ((Comparable) one).compareTo(two);
	}

	public static boolean getColumnTypeComparable(String tableName, String colName) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith(tableName + ",")) {
					String[] str = line.split(",");
					// check if the table has the column
					if (str[1].equals(colName)) {

						if (str[2].equals("java.util.Date")) {
							reader.close();
							return false;
						} else {
							reader.close();
							return true;
						}
					}
				}
			}
			reader.close();

		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		return false;
	}

	public static boolean performOperatorFunctionComparable(String colValue, String objValue, String operator) {

		switch (operator) {
		case "=":
			return colValue.compareTo(objValue) == 0;
		case "<":
			return colValue.compareTo(objValue) < 0;
		case ">":
			return colValue.compareTo(objValue) > 0;
		case "<=":
			return colValue.compareTo(objValue) < 0 || colValue.compareTo(objValue) == 0;
		case ">=":
			return colValue.compareTo(objValue) > 0 || colValue.compareTo(objValue) == 0;
		case "!=":
			return colValue.compareTo(objValue) != 0;
		}

		return false;
	}

	public static Comparable[] getXYZFromColNames(String[] colNames, Comparable[] colValues, String xName, String yName, String zName) {
		Comparable[] XYZ = new Comparable[3];
		for(int i=0;i<3;i++) {
			String colName = colNames[i];
			Comparable colValue = colValues[i];

			if(colName.equals(xName)) {
				XYZ[0] = colValue;
			} else if(colName.equals(yName)) {
				XYZ[1] = colValue;
			} else {
				XYZ[2] = colValue;
			}
		}

		return XYZ;
	}

	public static String[] getOperatorsFromColNames(String[] colNames, String[] operators, String xName, String yName, String zName) {
		String[] newOperators = new String[3];
		for(int i=0;i<3;i++) {
			String colName = colNames[i];

			if(colName.equals(xName)) {
				newOperators[0] = operators[i];
			} else if(colName.equals(yName)) {
				newOperators[1] = operators[i];
			} else {
				newOperators[2] = operators[i];
			}
		}

		return newOperators;
	}

	public static boolean performOperatorFunction(String colValue, String objValue, String operator)
			throws DBAppException {

		switch (operator) {
		case "=":
			return compareToDates(colValue, objValue) == 0;
		case "<":
			return compareToDates(colValue, objValue) < 0;
		case ">":
			return compareToDates(colValue, objValue) > 0;
		case "<=":
			return compareToDates(colValue, objValue) < 0 || compareToDates(colValue, objValue) == 0;
		case ">=":
			return compareToDates(colValue, objValue) > 0 || compareToDates(colValue, objValue) == 0;
		case "!=":
			return compareToDates(colValue, objValue) != 0;
		}

		return false;
	}

	public static boolean doComparison(Comparable value1, Comparable value2, String operator) {
		boolean add = false;
		switch(operator) {
			case "=":
				add = value1.equals(value2);
				break;
			case "!=":
				add = !value1.equals(value2);
				break;
			case ">":
				add = value1.compareTo(value2) > 0;
				break;
			case ">=":
				add = value1.compareTo(value2) > 0 || value1.equals(value2);
				break;
			case "<":
				add = value1.compareTo(value2) < 0;
				break;
			case "<=":
				add = value1.compareTo(value2) < 0 || value1.equals(value2);
				break;
		}
		return add;
	}

	private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

	private static String enbase(int x) {
			int n = ALPHABET.length();
			if (x < n) {
				return String.valueOf(ALPHABET.charAt(x));
			}
			return enbase(x / n) + ALPHABET.charAt(x % n);
	}

		private static int debase(String x) {
			int n = ALPHABET.length();
			int result = 0;
			for (int i = 0; i < x.length(); i++) {
				char c = x.charAt(i);
				result += ALPHABET.indexOf(c) * Math.pow(n, x.length() - i - 1);
			}
			return result;
		}

		private static String pad(String x, int n) {
				int difference = n - x.length();
				String p = "a".repeat(difference);

				return x + p;
		}

		public static String average(String S, String T) {
			int N = Math.max(S.length(), T.length());

			if(S.length() > T.length()) {
				T = Utilities.pad(T, S.length());
			} else if(S.length() < T.length()) {
				S = Utilities.pad(S, T.length());
			}


			int[] a1 = new int[N + 1];

			for (int i = 0; i < N; i++) {
				a1[i + 1] = (int)S.charAt(i) - 97
						+ (int)T.charAt(i) - 97;
			}

			// Iterate from right to left
			// and add carry to next position
			for (int i = N; i >= 1; i--) {
				a1[i - 1] += (int)a1[i] / 26;
				a1[i] %= 26;
			}

			// Reduce the number to find the middle
			// string by dividing each position by 2
			for (int i = 0; i <= N; i++) {

				// If current value is odd,
				// carry 26 to the next index value
				if ((a1[i] & 1) != 0) {

					if (i + 1 <= N) {
						a1[i + 1] += 26;
					}
				}

				a1[i] = (int)a1[i] / 2;
			}

			StringBuilder finalS = new StringBuilder();

			for (int i = 1; i <= N; i++) {
				finalS.append((char) (a1[i] + 97));
			}
			return finalS.toString();
		}


}
