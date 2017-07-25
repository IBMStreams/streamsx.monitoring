//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.system;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.Process;
import java.lang.Runtime;
import java.lang.management.ManagementFactory;
import java.util.Date;
import com.ibm.lang.management.*;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

/**
 * GetSystemStatus delivers the CPU and Memory usage.
 * 
 */
public class GetSystemStatus {

	/**
	 * get CPU and Memory usage of running system.
	 * 
	 * @param detail
	 * boolean if true the function deliver all details.
	 * @return String result. CPU and Memory usage.
	 */
	public static SystemStatus getCpuMemUsage(boolean detail) {
		if (detail) {
			return getCpuMemUsage1(detail);
		}
		
		SystemStatus systemStatus = new SystemStatus();
		// IBM specific platform management interface for the Operating System on which the Java Virtual Machine is running. 
		// https://www.ibm.com/support/knowledgecenter/en/SSYKE2_8.0.0/com.ibm.java.api.80.doc/com.ibm.lang.management/com/ibm/lang/management/OperatingSystemMXBean.html
		OperatingSystemMXBean ibmMxBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		// Returns the "recent cpu usage" for the whole system.
		double systemCpuLoad = ibmMxBean.getSystemCpuLoad();
		
		double cpuUsage = 100.00 * ((double) systemCpuLoad);
		cpuUsage = Double.parseDouble(new DecimalFormat("##.##").format(cpuUsage));
		float CpuUsage = (float) cpuUsage;
		
		// Returns the amount of physical memory that is available on the system in bytes.
		long freePhysicalMemory = ibmMxBean.getFreePhysicalMemorySize() / 1024;
		
		// Returns the total available physical memory on the system in bytes.
		long MemTotal = ibmMxBean.getTotalPhysicalMemory() / 1024;
		long MemUsed = MemTotal - freePhysicalMemory;
		float MemUsage = 0;
		if (MemTotal > MemUsed) {
			MemUsage = ((float) MemUsed / (float) MemTotal) * 100;
			MemUsage = Float.parseFloat(new DecimalFormat("##.##").format(MemUsage));
		}
		String dateTime = getDateTime();
//		System.out.printf("%s %.2f  %.2f  %d  %d  %n", dateTime, CpuUsage, MemUsage, MemTotal, MemUsed);
		systemStatus.date = dateTime;
		systemStatus.CpuUsage = CpuUsage;
		systemStatus.MemUsage = MemUsage;
		systemStatus.MemTotal = (int) MemTotal;
		systemStatus.MemUsed = (int) MemUsed;

		return systemStatus;

	}

	/**
	 * get the current date and time of system
	 * 
	 * @return String DateTime
	 */
	public static String getDateTime() {

		DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
		Date date = new Date();
		// System.out.println(dateFormat.format(date));
		return dateFormat.format(date);
	}

	public static SystemStatus getCpuMemUsage1(boolean detail) {
		String s = "";
		String result = "";
		Process p;
		String Top = "";
		String Tasks = "";
		String Cpu = "";
		String Mem = "";
		float CpuUsage = 0;
		float MemUsage = 0;
		int MemTotal = 1;
		int MemUsed = 0;
		SystemStatus systemStatus = new SystemStatus();
		try {
			// run the linux top 2 times to get the CPU and Memory usage
			String cmd = "top -bn 2";
			p = Runtime.getRuntime().exec(cmd);
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			int i = 0;
			while ((s = br.readLine()) != null) {
				i++;
				if (i > 10) // take the top results from 2. run
				{
					if (s.indexOf("top -") >= 0) {
						Top = s;
					}

					if (s.indexOf("Tasks:") >= 0) {
						Tasks = s;
					}

					if (s.indexOf("Cpu(s)") >= 0) {
						Cpu = s;
					}

					if (s.indexOf("Mem:") >= 0) {
						Mem = s;
					}

				}
			}

			int position = Cpu.indexOf("%us");
			// System.out.println(Top);
			// System.out.println(Cpu);
			if (position > 5) {
				CpuUsage = Float.valueOf((Cpu.substring(position - 4, position)).trim());
			}
			position = Mem.indexOf("k total");
			// get the total memory

			if (position > 9) {
				MemTotal = Integer.valueOf((Mem.substring(position - 8, position)).trim());
			}
			position = Mem.indexOf("k used");
			// get the used memory
			if (position > 9) {
				MemUsed = Integer.valueOf((Mem.substring(position - 8, position)).trim());
			}
			// calculate the memory usage used/total %
			if (MemTotal > MemUsage) {
				MemUsage = ((float) MemUsed / (float) MemTotal) * 100;
				MemUsage = Float.parseFloat(new DecimalFormat("##.##").format(MemUsage));
			}
			p.waitFor();
			p.destroy();
		} catch (Exception e) {
		}
		// System.out.println("result: " + result);
		if (detail) {
			result = getDateTime() + "\n" + Top + "\n" + Tasks + "\n" + Cpu + "\n" + Mem + "\n";
			systemStatus.date = result;
		} else {
			systemStatus.date = getDateTime();
		}
		systemStatus.CpuUsage = CpuUsage;
		systemStatus.MemUsage = MemUsage;
		systemStatus.MemTotal = MemTotal;
		systemStatus.MemUsed = MemUsed;

		return systemStatus;
	}

}
