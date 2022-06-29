import rocksetConfigure from '@rockset/client';
import {
  DatasourceMetadataDto,
  ExecutionOutput,
  RocksetDatasourceConfiguration,
  DBActionConfiguration,
  IntegrationError,
  RawRequest
} from '@superblocksteam/shared';
import { BasePlugin, PluginExecutionProps } from '@superblocksteam/shared-backend';

export default class RocksetPlugin extends BasePlugin {
  pluginName = 'Rockset';
  async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<RocksetDatasourceConfiguration>): Promise<ExecutionOutput> {
    try {
      const rocksetClient = rocksetConfigure(datasourceConfiguration.apiKey as string);
      const resp = await rocksetClient.queries.query({
        sql: {
          query: actionConfiguration.body,
          parameters: []
        }
      });

      const ret = new ExecutionOutput();
      ret.output = resp.results;

      return ret;
    } catch (err) {
      throw new IntegrationError(`${this.pluginName} query failed, ${err.message}`);
    }
  }

  getRequest(actionConfiguration: DBActionConfiguration): RawRequest {
    return actionConfiguration?.body;
  }

  dynamicProperties(): string[] {
    return ['body'];
  }

  async metadata(datasourceConfiguration: RocksetDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    return {};
  }

  async test(datasourceConfiguration: RocksetDatasourceConfiguration): Promise<void> {
    try {
      const rocksetClient = rocksetConfigure(datasourceConfiguration.apiKey as string);
      await rocksetClient.queries.query({
        sql: {
          query: 'select CURRENT_DATE()',
          parameters: []
        }
      });
    } catch (err) {
      throw new IntegrationError(`Test ${this.pluginName} connection failed, ${err.message}`);
    }
  }
}
