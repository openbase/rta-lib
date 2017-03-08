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

import de.citec.csra.task.srv.ExecutorFactoryTaskHandler;
import de.citec.csra.task.srv.TaskServer;
import java.util.logging.Level;
import java.util.logging.Logger;
import rsb.RSBException;
import rsb.converter.DefaultConverterRepository;
import rsb.converter.ProtocolBufferConverter;
import rst.communicationpatterns.TaskStateType;

/**
 *
 * @author pholthau
 */
public class TaskServerExample {

	private final static Logger LOG = Logger.getLogger(TaskServerExample.class.getName());
	
	static {
		DefaultConverterRepository.getDefaultConverterRepository().addConverter(new ProtocolBufferConverter<>(TaskStateType.TaskState.getDefaultInstance()));
	}


	public static void main(String[] args){

		try {
			TaskServer server = new TaskServer("/example/scope", new ExecutorFactoryTaskHandler((description) -> () -> description));
			server.execute();
			server.deactivate();
		} catch (InterruptedException | RSBException ex) {
			LOG.log(Level.SEVERE, "Error in task server", ex);
		}
	}
}
