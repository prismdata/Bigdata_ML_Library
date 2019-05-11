/*
 * Copyright (C) 2011 ankus (http://www.openankus.org).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.ankus.mapreduce.algorithms.classification.C45;

import java.util.Comparator;

public class Instance {
	public double variable;
	public String classeLabel;
	public Double getVariable()
	{
		return variable;
	}
}

class MemberComparator implements Comparator<Instance>{
	  public int compare(Instance arg0, Instance arg1) 
	  {
		  return arg0.getVariable().compareTo(arg1.getVariable());
		  			//arg1.getName().compareTo(arg0.getName());
	 }
}