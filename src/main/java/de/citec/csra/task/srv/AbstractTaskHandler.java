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
package de.citec.csra.task.srv;

import de.citec.csra.task.TaskProxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import rsb.Event;
import rsb.Informer;
import rsb.RSBException;
import rst.communicationpatterns.TaskStateType.TaskState;

/**
 *
 * @author Patrick Holthaus
 * (<a href=mailto:patrick.holthaus@uni-bielefeld.de>patrick.holthaus@uni-bielefeld.de</a>)
 */
public abstract class AbstractTaskHandler implements TaskHandler, LocalTaskFactory {

	private final ExecutorService service = Executors.newCachedThreadPool();

	@Override
	public void handle(TaskState t, Event e, Informer i) throws RSBException, InterruptedException {
		TaskProxy proxy = new TaskProxy(t, e, i);
		TaskExecutionMonitor monitor = new TaskExecutionMonitor(proxy, this);
		service.submit(monitor);
	}

	@Override
	public abstract LocalTask newLocalTask(Object description) throws IllegalArgumentException;

}
