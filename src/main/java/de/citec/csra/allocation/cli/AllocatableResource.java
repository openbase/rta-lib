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
package de.citec.csra.allocation.cli;

import de.citec.csra.rst.util.IntervalUtils;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import rsb.RSBException;
import rst.communicationpatterns.ResourceAllocationType.ResourceAllocation;
import rst.communicationpatterns.ResourceAllocationType.ResourceAllocation.Initiator;
import rst.communicationpatterns.ResourceAllocationType.ResourceAllocation.Policy;
import rst.communicationpatterns.ResourceAllocationType.ResourceAllocation.Priority;
import rst.communicationpatterns.ResourceAllocationType.ResourceAllocation.State;
import static rst.communicationpatterns.ResourceAllocationType.ResourceAllocation.State.REQUESTED;

/**
 *
 * @author Patrick Holthaus
 * (<a href=mailto:patrick.holthaus@uni-bielefeld.de>patrick.holthaus@uni-bielefeld.de</a>)
 */
public class AllocatableResource implements SchedulerListener, Executable {

	private final static Logger LOG = Logger.getLogger(ExecutableResource.class.getName());
	private final RemoteAllocation remote;
	private final LinkedBlockingDeque<State> queue = new LinkedBlockingDeque<>();
	private final Object monitor = new Object();

	public AllocatableResource(ResourceAllocation allocation) {
		this.remote = new RemoteAllocation(ResourceAllocation.newBuilder(allocation));
	}

	public AllocatableResource(String description, Policy policy, Priority priority, Initiator initiator, long delay, long duration, String... resources) {
		this.remote = new RemoteAllocation(ResourceAllocation.newBuilder().
				setInitiator(initiator).
				setPolicy(policy).
				setPriority(priority).
				setDescription(description).
				setSlot(IntervalUtils.buildRelativeRst(delay, duration)).
				addAllResourceIds(Arrays.asList(resources)));
	}

	@Override
	public void startup() throws RSBException {
		if (this.queue.isEmpty()) {
			this.queue.add(this.remote.getCurrentState());
			this.remote.addSchedulerListener(this);
			this.remote.schedule();
		} else {
			LOG.log(Level.WARNING, "Startup called while already active ({0}), ignoring.", getState());
		}
	}

	@Override
	public void shutdown() throws RSBException {
		switch (getState()) {
			case REQUESTED:
			case SCHEDULED:
				remote.cancel();
				this.remote.removeSchedulerListener(this);
				break;
			case ALLOCATED:
				remote.abort();
				this.remote.removeSchedulerListener(this);
				break;
			default:
				LOG.log(Level.WARNING, "Shutdown called in inactive state ({0}), ignoring.", getState());
				break;
		}
	}

	@Override
	public void allocationUpdated(ResourceAllocation allocation) {
		synchronized (this.monitor) {
			this.queue.add(allocation.getState());
			this.monitor.notifyAll();
		}
	}

	public RemoteAllocation getRemote() {
		return this.remote;
	}

	public State getState() {
		return this.queue.peekLast();
	}
	
	public boolean hasState(State state) {
		return this.queue.contains(state);
	}

	public void await(State state) throws InterruptedException {
		synchronized (this.monitor) {
			while (!this.queue.contains(state)) {
				this.monitor.wait();
			}
		}
	}

	public void await(State state, long timeout) throws InterruptedException, TimeoutException {
		synchronized (this.monitor) {
			if (this.queue.contains(state)) {
				return;
			}
			long start = System.currentTimeMillis();
			long remaining = timeout;
			while (remaining > 0) {
				this.monitor.wait(remaining);
				if (this.queue.contains(state)) {
					return;
				} else {
					remaining = timeout - (System.currentTimeMillis() - start);
				}
			}
			throw new TimeoutException("Waiting for state '" + state.name() + "' timed out after " + timeout + "ms.");
		}
	}
}
