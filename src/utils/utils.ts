/**
 * @license
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import { requestAPI } from '../handler';

import { DataprocLoggingService } from './loggingService';
// import { showLoginDialog } from './loginPopup';
export interface IAuthCredentials {
  access_token?: string;
  project_id?: string;
  region_id?: string;
  config_error?: number;
  login_error?: number;
}

export const authApi = async (
): Promise<IAuthCredentials | undefined> => {
  try {
    const data = await requestAPI('credentials');
    if (typeof data === 'object' && data !== null) {
      const credentials: IAuthCredentials = {
        access_token: (data as { access_token: string }).access_token,
        project_id: (data as { project_id: string }).project_id,
        region_id: (data as { region_id: string }).region_id,
        config_error: (data as { config_error: number }).config_error,
        login_error: (data as { login_error: number }).login_error
      };
      // if (checkApiEnabled) {
      //   if (credentials.login_error || credentials.config_error) {
      //     try {
      //       const dialogResult = await showLoginDialog({
      //         loginError: credentials.login_error === 1,
      //         configError: credentials.config_error === 1
      //       });
      //       if (dialogResult) {
      //         return await authApi();
      //       } else {
      //         console.log('cance', dialogResult);
      //         return credentials;
      //       }
      //     } catch (dialogError) {
      //       console.error('Dialog was cancelled or failed:', dialogError);
      //       return credentials;
      //     }
      //   } else {
      //     console.error('Invalid data format.');
      //   }
      // }
      return credentials
    }
  } catch (reason) {
    console.error(`Error on GET credentials.\n${reason}`);
  }
};

/**
 * Wraps a fetch call with initial authentication to pass credentials to the request
 *
 * @param uri the endpoint to call e.g. "/clusters"
 * @param method the HTTP method used for the request
 * @param regionIdentifier option param to define what region identifier (location, region) to use
 * @param queryParams
 * @returns a promise of the fetch result
 */




/**
 * Helper method that wraps fetch and logs the request uri and status codes to
 * jupyter server.
 */
export async function loggedFetch(
  input: RequestInfo | URL,
  init?: RequestInit
): Promise<Response> {
  const resp = await fetch(input, init);
  // Intentionally not waiting for log response.
  DataprocLoggingService.logFetch(input, init, resp);
  return resp;
}

