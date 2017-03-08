/* 
 * Copyright (C) 2016 Bielefeld University, Patrick Holthaus
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
package de.citec.csra.task.cli;

import de.citec.csra.task.TaskProxy;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import rsb.InitializeException;
import rsb.RSBException;
import rst.communicationpatterns.TaskStateType.TaskState;
import rst.communicationpatterns.TaskStateType.TaskState.State;
import static rst.communicationpatterns.TaskStateType.TaskState.State.ABORT;
import static rst.communicationpatterns.TaskStateType.TaskState.State.ACCEPTED;
import static rst.communicationpatterns.TaskStateType.TaskState.State.COMPLETED;
import static rst.communicationpatterns.TaskStateType.TaskState.State.INITIATED;

/**
 *
 * @author Patrick Holthaus
 * (<a href=mailto:patrick.holthaus@uni-bielefeld.de>patrick.holthaus@uni-bielefeld.de</a>)
 */
public class RemoteTask<T> implements Callable, TaskListener {

	private final long accept;
	private final T payload;
	private final TaskProxy proxy;
	private final String scope;
	private final Object monitor = new Object();
	private State state = INITIATED;

	public RemoteTask(String scope, T payload) throws InitializeException {
		this(scope, payload, 1000);
	}

	public RemoteTask(String scope, T payload, long accept) throws InitializeException {
		this.scope = scope;
		this.proxy = new TaskProxy(scope);
		this.payload = payload;
		this.accept = accept;
	}

	private void activate() throws RSBException, InterruptedException {
		synchronized (this.monitor) {
			this.proxy.addTaskListener(this);
			this.proxy.activate();
			this.proxy.update(state, payload);
		}
	}

	private void deactivate() throws InterruptedException, RSBException {
		this.proxy.removeTaskListener(this);
		this.proxy.deactivate();
	}

	@Override
	public Object call() throws RSBException, InterruptedException, TimeoutException {

		activate();

		try {

			synchronized (this.monitor) {
				this.monitor.wait(accept);
			}

			switch (state) {
				case ACCEPTED:
					while (true) {
						synchronized (this.monitor) {
							this.monitor.wait();
							switch (state) {
//								still computing:
								case ABORT_FAILED:
								case ACCEPTED:
								case RESULT_AVAILABLE:
									break;
//								success:
								case COMPLETED:
									return proxy.getPayload();
//								error states:
								case ABORTED:
								case FAILED:
								case UPDATE_REJECTED:
									throw new RuntimeException("Task at '" + scope + "' ended abnormally (" + state + "): " + proxy.getPayload());
//								illegal states:
								case ABORT: //should only be sent by client.
								case UPDATE: //should only be sent by client.
								case INITIATED: //already accepted, doesn't make sense here.
									this.proxy.update(ABORT);
									deactivate();
								case REJECTED: //already accepted, doesn't make sense here.
									throw new IllegalArgumentException("Received illegal task state '" + state + "' at '" + scope + "', aborting and shutting down.");
							}
						}
					}
				case REJECTED:
					throw new RuntimeException("Task at '" + scope + "' could not be executed (" + state + "): " + proxy.getPayload());
				case INITIATED:
					deactivate();
					throw new TimeoutException("Task at '" + scope + "' timed out.");
				default:
					this.proxy.update(ABORT);
					deactivate();
					throw new IllegalArgumentException("Received illegal task state '" + state + "' at '" + scope + "', aborting and shutting down.");
			}
		} catch (InterruptedException ex) {
			deactivate();
			throw ex;
		}
	}

	@Override
	public void updated(TaskState state) {
		synchronized (this.monitor) {
			this.state = state.getState();
			this.monitor.notifyAll();
		}
	}
}
