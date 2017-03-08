/*
 * Copyright (C) 2017 pholthau
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
package de.citec.csra.task;

import de.citec.csra.task.cli.RemoteTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import rsb.InitializeException;

/**
 *
 * @author pholthau
 */
public class RemoteTaskExample {

	private final static Logger LOG = Logger.getLogger(RemoteTaskExample.class.getName());

	public static void main(String[] args) {
		try {
			RemoteTask t = new RemoteTask("/example/scope", "string-payload");
			ExecutorService ex = Executors.newSingleThreadExecutor();
			Future f = ex.submit(t);
			try {
				System.out.println("result: " + f.get());
			} catch (InterruptedException | ExecutionException e) {
				LOG.log(Level.SEVERE, "Task execution aborted", e);
			}
			ex.shutdownNow();
		} catch (InitializeException ex) {
			LOG.log(Level.SEVERE, "Could not initialize remote task", ex);
		}
	}
}
