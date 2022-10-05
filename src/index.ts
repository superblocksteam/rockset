import rocksetConfigure from '@rockset/client';
import {
  DatasourceMetadataDto,
  ExecutionOutput,
  RocksetDatasourceConfiguration,
  DBActionConfiguration,
  IntegrationError,
  RawRequest
} from '@superblocksteam/shared';
import { DatabasePlugin, PluginExecutionProps } from '@superblocksteam/shared-backend';

export default class RocksetPlugin extends DatabasePlugin {
  pluginName = 'Rockset';
  public async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<RocksetDatasourceConfiguration>): Promise<ExecutionOutput> {
    try {
      const rocksetClient = rocksetConfigure(datasourceConfiguration.apiKey as string);
      const resp = await this.executeQuery(() => {
        return rocksetClient.queries.query({
          sql: {
            query: actionConfiguration.body ?? '',
            parameters: []
          }
        });
      });

      const ret = new ExecutionOutput();
      ret.output = resp.results;

      return ret;
    } catch (err) {
      throw new IntegrationError(`${this.pluginName} query failed, ${err.message}`);
    }
  }

  public getRequest(actionConfiguration: DBActionConfiguration): RawRequest {
    return actionConfiguration?.body;
  }

  public dynamicProperties(): string[] {
    return ['body'];
  }

  public async metadata(datasourceConfiguration: RocksetDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    return {};
  }

  public async test(datasourceConfiguration: RocksetDatasourceConfiguration): Promise<void> {
    try {
      const rocksetClient = rocksetConfigure(datasourceConfiguration.apiKey as string);
      await this.executeQuery(() => {
        return rocksetClient.queries.query({
          sql: {
            query: 'select CURRENT_DATE()',
            parameters: []
          }
        });
      });
    } catch (err) {
      throw new IntegrationError(`Test ${this.pluginName} connection failed, ${err.message}`);
    }
  }
}
